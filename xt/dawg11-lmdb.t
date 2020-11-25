#!/usr/bin/env perl

use v5.14;
use strict;
use File::Temp qw(tempdir);
use warnings;
no warnings 'redefine';
no warnings 'once';

binmode(\*STDERR, ':encoding(utf8)');
binmode(\*STDOUT, ':encoding(utf8)');

use autodie;
use Test::Roo;
use List::MoreUtils qw(all);
use Types::Standard qw(Str);
use namespace::clean;

with 'Test::Attean::SPARQLSuite';
has filename => (is => 'ro', isa => Str);

my %args;
while (defined(my $opt = shift)) {
	if ($opt eq '-v') {
		$args{debug}++;
	} else {
		$args{pattern}	= $opt;
	}
}
run_me(\%args);

done_testing;

sub test_model {
	my $path = tempdir(CLEANUP => 1);
	my $store = Attean->get_store('LMDB')->new(filename => $path, initialize => 1);
	my $model	= Attean::MutableQuadModel->new( store => $store );
	return $model;
}
