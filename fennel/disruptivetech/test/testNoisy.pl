#!/usr/bin/perl

# ---
# $Id$
# Fennel is a library of data storage and processing components.
# Copyright (C) 2004-2007 Disruptive Tech
# Copyright (C) 2005-2007 The Eigenbase Project
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version approved by The Eigenbase Project.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# ---

# Use this way:
# $ ./testNoisy.pl > testNoisyArithmeticGen.hpp
# $ cat testNoisyArithmeticGen.hpp | ./testNoisyArithmetic -
# $ ./testNoisy.pl | ./testNoisyArithmetic -

# change the do_char_type line at the bottom of to control the
# amount of tests generated for char type

	print <<__EOT__
EXPR(":# ---")
EXPR(":# AUTOMATICALLY GENERATED FILE. DO NOT MODIFY")
EXPR(":# ---")
EXPR(":")
__EOT__
;

# ---
sub print_line
{
	$verb = shift;
	print "EXPR(\":$verb:$type:";
	if ( $r < $min || $r > $max ) {
		print "!22003";
		}
	else {
		print "$r";
		}
	print ":$i:$j\")\n";
}

# ---
sub do_char_type
{
	$type = shift;
	$min = shift;
	$max = shift;

	# ---
	print <<__EOT__
EXPR(":# ---")
EXPR(":# test limits for "$type" limits $min to $max")
EXPR(":# ---")
__EOT__
;
	for( $i=$min; $i<=$max; $i++ ) {
		for( $j=$min; $j<=$max; $j++ ) {

			$r = $i+$j;
			print_line("add");

			$r = $i-$j;
			print_line("sub");

			$r = $i*$j;
			print_line("mul");

			if ( !$j ) {
				$r = "!22012";
				}
			else {
				$r = int($i/$j);
				}
			print_line("div");
			}
		}
}

# ---
sub d_type
{
	$type = shift;
	$min1 = shift;
	$max1 = shift;
	$min2 = shift;
	$max2 = shift;

	# ---
	for( $i=$min1; $i<=$max1; $i++ ) {
		for( $j=$min2; $j<=$max2; $j++ ) {

			$r = $i+$j;
			print_line("add");

			$r = $i-$j;
			print_line("sub");

			$r = $i*$j;
			print_line("mul");

			if ( !$j ) {
				$r = "!22012";
				}
			else {
				$r = int($i/$j);
				}
			print_line("div");
			}
		}
}

# ---
$range = 4;
sub do_type
{
	$type = shift;
	$min = shift;
	$max = shift;
	if ( $min < 0 ) {
		# signed
		$mid = int($max / 2);
		$hange = int($range / 2);

		d_type( $type, $min, $min+$range, $min, $min+range );
		d_type( $type, $min, $min+$range, -$mid-$hange, -$mid+hange );
		d_type( $type, $min, $min+$range, 0-$hange, 0+hange );
		d_type( $type, $min, $min+$range, $mid-$hange, $mid+hange );
		d_type( $type, $min, $min+$range, $max-$range, $max );

		d_type( $type, -$mid-$hange, -$mid+$hange, $min, $min+range );
		d_type( $type, -$mid-$hange, -$mid+$hange, -$mid-$hange, -$mid+hange );
		d_type( $type, -$mid-$hange, -$mid+$hange, 0-$hange, 0+hange );
		d_type( $type, -$mid-$hange, -$mid+$hange, $mid-$hange, $mid+hange );
		d_type( $type, -$mid-$hange, -$mid+$hange, $max-$range, $max );

		d_type( $type, 0-$hange, 0+$hange, $min, $min+range );
		d_type( $type, 0-$hange, 0+$hange, -$mid-$hange, -$mid+hange );
		d_type( $type, 0-$hange, 0+$hange, 0-$hange, 0+hange );
		d_type( $type, 0-$hange, 0+$hange, $mid-$hange, $mid+hange );
		d_type( $type, 0-$hange, 0+$hange, $max-$range, $max );

		d_type( $type, $mid-$hange, $mid+$hange, $min, $min+range );
		d_type( $type, $mid-$hange, $mid+$hange, -$mid-$hange, -$mid+hange );
		d_type( $type, $mid-$hange, $mid+$hange, 0-$hange, 0+hange );
		d_type( $type, $mid-$hange, $mid+$hange, $mid-$hange, $mid+hange );
		d_type( $type, $mid-$hange, $mid+$hange, $max-$range, $max );

		d_type( $type, $max-$range, $max, $min, $min+range );
		d_type( $type, $max-$range, $max -$mid-$hange, -$mid+hange );
		d_type( $type, $max-$range, $max, 0-$hange, 0+hange );
		d_type( $type, $max-$range, $max, $mid-$hange, $mid+hange );
		d_type( $type, $max-$range, $max, $max-$range, $max );

		}
	else {
		# unsigned
		$mid = int($max / 2);
		$hange = int($range / 2);

		d_type( $type, $min, $min+$range, $min, $min+range );
		d_type( $type, $min, $min+$range, $mid-$hange, $mid+hange );
		d_type( $type, $min, $min+$range, $max-$range, $max );

		d_type( $type, $mid-$hange, $mid+$hange, $min, $min+range );
		d_type( $type, $mid-$hange, $mid+$hange, $mid-$hange, $mid+hange );
		d_type( $type, $mid-$hange, $mid+$hange, $max-$range, $max );

		d_type( $type, $max-$range, $max, $min, $min+range );
		d_type( $type, $max-$range, $max, $mid-$hange, $mid+hange );
		d_type( $type, $max-$range, $max, $max-$range, $max );

		}
}

# ---
sub do_float_types
{
	print <<__EOT__
EXPR(":# ---")
EXPR(":# test various floating point types")
EXPR(":# ---")
EXPR(":")
EXPR(":# --- normal stuff")
EXPR(":add:float:3.0:2.0:1.0")
EXPR(":sub:float:1.0:2.0:1.0")
EXPR(":mul:float:4.0:2.0:2.0")
EXPR(":div:float:2.0:4.0:2.0")
EXPR(":add:double:30.0:20.0:10.0")
EXPR(":sub:double:10.0:20.0:10.0")
EXPR(":mul:double:400.0:20.0:20.0")
EXPR(":div:double:2.0:40.0:20.0")
EXPR(":add:long double:300.0:200.0:100.0")
EXPR(":sub:long double:100.0:200.0:100.0")
EXPR(":mul:long double:40000.0:200.0:200.0")
EXPR(":div:long double:2.0:400.0:200.0")
EXPR(":")
EXPR(":# --- error conditions")
EXPR(":div:float:!22012:1.0:0.0")
EXPR(":div:float:!22023:0.0:0.0")
EXPR(":add:float:!22000:\$MAX:1.0")
EXPR(":add:float:!22003:\$MAX:\$MAX:")
EXPR(":div:float:!22000:.00001:\$MAX:")
EXPR(":")
EXPR(":div:double:!22012:1.0:0.0")
EXPR(":div:double:!22023:0.0:0.0")
EXPR(":add:double:!22000:\$MAX:1.0")
EXPR(":add:double:!22003:\$MAX:\$MAX:")
EXPR(":div:double:!22000:.00001:\$MAX:")
EXPR(":")
EXPR(":div:long double:!22012:1.0:0.0")
EXPR(":div:long double:!22023:0.0:0.0")
EXPR(":add:long double:!22000:\$MAX:1.0")
EXPR(":add:long double:!22003:\$MAX:\$MAX:")
EXPR(":div:long double:!22000:.00001:\$MAX:")
EXPR(":")
__EOT__
;
}

# --- very verbose (500K+ lines)
#do_char_type( "char", -128, 127 );
#do_char_type( "signed char", -128, 127 );
#do_char_type( "unsigned char", 0, 255 );
# --- not so verbose
do_type( "char", -128, 127 );
do_type( "signed char", -128, 127 );
do_type( "unsigned char", 0, 255 );

# ---
do_type( "short", -32768, 32767 );
do_type( "unsigned short", 0, 65535 );
do_type( "int", -2147483648, 2147483647 );
do_type( "unsigned int", 0, 4294967295 );
#perl doesn't handle 64 well
#do_type( "long long unsigned int", -9223372036854775808, 9223372036854775807 );
#do_type( "long long unsigned int", 0, 18446744073709551615 );

# ---
do_float_types();

