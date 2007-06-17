/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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
 * RelOptCostImpl provides a default implementation for the {@link RelOptCost}
 * interface. It it defined in terms of a single scalar quantity; somewhat
 * arbitrarily, it returns this scalar for rows processed and zero for both CPU
 * and I/O.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelOptCostImpl
    implements RelOptCost
{
    //~ Instance fields --------------------------------------------------------

    private final double value;

    //~ Constructors -----------------------------------------------------------

    public RelOptCostImpl(double value)
    {
        this.value = value;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptCost
    public double getRows()
    {
        return value;
    }

    // implement RelOptCost
    public double getIo()
    {
        return 0;
    }

    // implement RelOptCost
    public double getCpu()
    {
        return 0;
    }

    // implement RelOptCost
    public boolean isInfinite()
    {
        return Double.isInfinite(value);
    }

    // implement RelOptCost
    public boolean isLe(RelOptCost other)
    {
        return getRows() <= other.getRows();
    }

    // implement RelOptCost
    public boolean isLt(RelOptCost other)
    {
        return getRows() < other.getRows();
    }

    // implement RelOptCost
    public boolean equals(RelOptCost other)
    {
        return getRows() == other.getRows();
    }

    // implement RelOptCost
    public RelOptCost minus(RelOptCost other)
    {
        return new RelOptCostImpl(getRows() - other.getRows());
    }

    // implement RelOptCost
    public RelOptCost plus(RelOptCost other)
    {
        return new RelOptCostImpl(getRows() + other.getRows());
    }

    // implement RelOptCost
    public RelOptCost multiplyBy(double factor)
    {
        return new RelOptCostImpl(getRows() * factor);
    }

    // implement RelOptCost
    public String toString()
    {
        if (value == Double.MAX_VALUE) {
            return "huge";
        } else {
            return Double.toString(value);
        }
    }
}

// End RelOptCostImpl.java
