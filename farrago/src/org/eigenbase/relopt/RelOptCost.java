/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.eigenbase.relopt;


/**
 * todo:
 */
public interface RelOptCost
{
    //~ Methods ---------------------------------------------------------------

    abstract double getCpu();

    abstract boolean isInfinite();

    abstract double getIo();

    abstract boolean isLe(RelOptCost cost);

    abstract boolean isLt(RelOptCost cost);

    abstract double getRows();

    abstract boolean equals(RelOptCost cost);

    abstract RelOptCost minus(RelOptCost cost);

    abstract RelOptCost multiplyBy(double factor);

    abstract RelOptCost plus(RelOptCost cost);

    abstract String toString();
}


// End RelOptCost.java
