=head1 NAME

AtteanX::Store::LMDB - LMDB-based RDF store

=head1 VERSION

This document describes AtteanX::Store::LMDB version 0.026

=head1 SYNOPSIS

 use AtteanX::Store::LMDB;

=head1 DESCRIPTION

AtteanX::Store::LMDB provides a persistent quad-store based on LMDB.

=cut

use v5.14;
use warnings;

package AtteanX::Store::LMDB 0.026 {
use Moo;
use Type::Tiny::Role;
use Types::Standard qw(Str Int ArrayRef HashRef ConsumerOf InstanceOf);
use LMDB_File qw(:flags :cursor_op);
use Encode;
use Set::Scalar;
use Digest::SHA qw(sha256 sha256_hex);
use List::Util qw(first);
use Scalar::Util qw(refaddr reftype blessed);
use Math::Cartesian::Product;
use Devel::Peek;
use namespace::clean;

with 'Attean::API::QuadStore';

my @pos_names	= Attean::API::Quad->variables;

=head1 ATTRIBUTES

=over 4

=item C<< subject >>

=item C<< predicate >>

=item C<< object >>

=item C<< graph >>

=back

=head1 METHODS

Beyond the methods documented below, this class inherits methods from the
L<Attean::API::QuadStore> class.

=over 4

=item C<< new () >>

Returns a new memory-backed storage object.

=cut

has filename => (is => 'ro', isa => Str, required => 1);
has env		=> (is => 'rw', isa => InstanceOf['LMDB::Env']);

sub BUILD {
	my $self	= shift;
	my $file	= $self->filename;
	
	my $env = LMDB::Env->new($file, {
		mapsize => 100 * 1024 * 1024 * 1024, # Plenty space, don't worry
		maxdbs => 20, # Some databases
		mode   => 0640,
	});
	$self->env($env);
}

=item C<< size >>

Returns the number of quads in the store.

=cut

sub size {
	my $self	= shift;
	my $txn		= $self->env->BeginTxn(MDB_RDONLY);
	my $quads	= $txn->OpenDB({ dbname => 'quads', });
	my $stat	= $quads->stat // {};
	return $stat->{'entries'} // 0;
}

=item C<< get_quads ( $subject, $predicate, $object, $graph ) >>

Returns a stream object of all statements matching the specified subject,
predicate and objects. Any of the arguments may be undef to match any value.

=cut

sub get_quads {
	my $self	= shift;
	my @nodes	= map { ref($_) eq 'ARRAY' ? $_ : [$_] } @_;
	my @iters;
	cartesian { push(@iters, $self->_get_quads(@_)) } @nodes;
	return Attean::IteratorSequence->new( iterators => \@iters, item_type => 'Attean::API::Quad' );
}

sub _encode_term {
	my $self	= shift;
	my $term	= shift;
	my $value	= $term->value;
	if ($term->isa('Attean::IRI')) {
		return 'I"' . $value;
	} elsif ($term->isa('Attean::Literal')) {
		if (my $dt = $term->datatype) {
		} elsif (my $lang = $term->language) {
		} else {
		}
		die Dumper($term);
	} elsif ($term->isa('Attean::Blank')) {
		return 'B"' . $value;
	}
}
sub _parse_term {
	my $self	= shift;
	my $data	= shift;
	my $type	= substr($data, 0, 1);
	if ($type eq 'I') {
		my $value	= substr($data, 2);
		return Attean::IRI->new(value => $value);
	} elsif ($type eq 'S') {
		my $value	= substr($data, 2);
		return Attean::Literal->new(value => $value);
	} elsif ($type eq 'D') {
		my $i		= index($data, '"');
		my $dt		= substr($data, 1, $i-1);
		my $value	= substr($data, $i+1);
		return Attean::Literal->new(value => $value, datatype => $dt);
	} elsif ($type eq 'i') {
		my $value	= substr($data, 2);
		return Attean::Literal->new(value => $value, datatype => 'http://www.w3.org/2001/XMLSchema#integer');
	} elsif ($type eq 'B') {
		my $value	= substr($data, 2);
		return Attean::Blank->new($value);
	} else {
		Dump($data);
		die;
	}
	return;
}

sub _get_term_id {
	my $self	= shift;
	my $term	= shift;
	my $t2i		= shift;
	my $encoded		= $self->_encode_term($term);
	my $hash		= sha256($encoded);
	my $value		= $t2i->get($hash);
	unless ($value) {
		return;
	}
	my ($id)		= unpack('Q>', $value);
	return $id;
}

sub _get_quads {
	my $self	= shift;
	my @nodes	= @_;
	my $bound	= 0;
	my %bound;
	
	{
		my $txn		= $self->env->BeginTxn(MDB_RDONLY);
		my $t2i		= $txn->OpenDB({ dbname => 'term_to_id', });
		foreach my $pos (0 .. 3) {
			my $n	= $nodes[ $pos ];
			if (blessed($n) and $n->does('Attean::API::Variable')) {
				$n	= undef;
				$nodes[$pos]	= undef;
			}
			if (blessed($n)) {
				$bound++;
				my $id			= $self->_get_term_id($n, $t2i);
				unless ($id) {
					warn "No such term in quadstore: " . $n->as_string;
					return Attean::ListIterator->new( values => [], item_type => 'Attean::API::Quad' );
				}
				$bound{ $pos }	= $id;
			}
		}
	}
	
	my $txn		= $self->env->BeginTxn(MDB_RDONLY);
	my $quads	= $txn->OpenDB({ dbname => 'quads', });
	if ($bound == 0) {
		my $cursor	= $quads->Cursor;
		my ($key, $value);
		my @quadids;
		eval {
			local($LMDB_File::die_on_err)	= 0;
			unless ($cursor->get($key, $value, MDB_FIRST)) {
				while (1) {
					my (@ids)	= unpack('Q>4', $value);
					push(@quadids, \@ids);
					last if $cursor->get($key, $value, MDB_NEXT);
				}
			}
		};
		
		my $sub	= sub {
			my $txn		= $self->env->BeginTxn(MDB_RDONLY);
			my $i2t		= $txn->OpenDB({ dbname => 'id_to_term', });
			QUAD: while (my $tids = shift(@quadids)) {
				my @terms;
				foreach my $tid (@$tids) {
					my $key	= pack('Q>', $tid);
					my $termdata = $i2t->get($key);
					next QUAD unless ($termdata);
					my $term	= $self->_parse_term($termdata);
					next QUAD unless ($term);
					push(@terms, $term);
				}
				if (scalar(@terms) == 4) {
					return Attean::Quad->new(@terms);
				}
			}
			return;
		};
		return Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Quad' );
	} else {
		my $cursor	= $quads->Cursor;
		my ($key, $value);
		my @quadids;
		eval {
			local($LMDB_File::die_on_err)	= 0;
			unless ($cursor->get($key, $value, MDB_FIRST)) {
				QUAD: while (1) {
					my (@ids)	= unpack('Q>4', $value);
					foreach my $k (keys %bound) {
						my $id	= $bound{$k};
						unless ($id == $ids[$k]) {
							next QUAD;
						}
					}
					push(@quadids, \@ids);
				} continue {
					last if $cursor->get($key, $value, MDB_NEXT);
				}
			}
		};
		
		my $sub	= sub {
			my $txn		= $self->env->BeginTxn(MDB_RDONLY);
			my $i2t		= $txn->OpenDB({ dbname => 'id_to_term', });
			QUAD: while (my $tids = shift(@quadids)) {
				my @terms;
				foreach my $tid (@$tids) {
					my $key	= pack('Q>', $tid);
					my $termdata = $i2t->get($key);
					next QUAD unless ($termdata);
					my $term	= $self->_parse_term($termdata);
					next QUAD unless ($term);
					push(@terms, $term);
				}
				if (scalar(@terms) == 4) {
					return Attean::Quad->new(@terms);
				}
			}
			return;
		};
		return Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Quad' );
	}
}

=item C<< get_graphs >>

Returns an iterator over the Attean::API::Term objects comprising
the set of graphs of the stored quads.

=cut

sub get_graphs {
	my $self	= shift;
	my $txn		= $self->env->BeginTxn(MDB_RDONLY);
	my $graphs	= $txn->OpenDB({ dbname => 'graphs', });
	my $i2t		= $txn->OpenDB({ dbname => 'id_to_term', });
	my $cursor	= $graphs->Cursor;
	my ($key, $value);
	my @graph_ids;
	eval {
		local($LMDB_File::die_on_err)	= 0;
		unless ($cursor->get($key, $value, MDB_FIRST)) {
			while (1) {
				my ($gid)	= unpack('Q>', $key);
				push(@graph_ids, $gid);
				last if $cursor->get($key, $value, MDB_NEXT);
			}
		}
	};
	
	my $sub	= sub {
		my $txn		= $self->env->BeginTxn(MDB_RDONLY);
		my $i2t		= $txn->OpenDB({ dbname => 'id_to_term', });
		GRAPH: while (my $gid = shift(@graph_ids)) {
			my $key	= pack('Q>', $gid);
			my $termdata = $i2t->get($key);
			next QUAD unless ($termdata);
			my $term	= $self->_parse_term($termdata);
			next GRAPH unless ($term);
			return $term;
		}
		return;
	};
	return Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Term' );
}
}

1;

__END__

=back

=head1 BUGS

Please report any bugs or feature requests to through the GitHub web interface
at L<https://github.com/kasei/perlrdf2/issues>.

=head1 AUTHOR

Gregory Todd Williams  C<< <gwilliams@cpan.org> >>

=head1 SEE ALSO

=over 4

=item * L<Diomede|https://github.com/kasei/diomede/> is a Swift LMDB-based quadstore that shares the same file format as this module.

=back

=head1 COPYRIGHT

Copyright (c) 2020 Gregory Todd Williams. This
program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
