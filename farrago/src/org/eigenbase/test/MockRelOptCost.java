/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.test;

import org.eigenbase.relopt.*;


/**
 * MockRelOptCost is a mock implementation of the {@link RelOptCost} interface.
 * TODO: constructors for various scenarios
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MockRelOptCost
    implements RelOptCost
{
    //~ Methods ----------------------------------------------------------------

    public double getCpu()
    {
        return 0;
    }

    public boolean isInfinite()
    {
        return false;
    }

    public double getIo()
    {
        return 0;
    }

    public boolean isLe(RelOptCost cost)
    {
        return true;
    }

    public boolean isLt(RelOptCost cost)
    {
        return false;
    }

    public double getRows()
    {
        return 0;
    }

    public boolean equals(RelOptCost cost)
    {
        return true;
    }

    public RelOptCost minus(RelOptCost cost)
    {
        return this;
    }

    public RelOptCost multiplyBy(double factor)
    {
        return this;
    }

    public RelOptCost plus(RelOptCost cost)
    {
        return this;
    }

    public String toString()
    {
        return "MockRelOptCost(0)";
    }
}

// End MockRelOptCost.java
