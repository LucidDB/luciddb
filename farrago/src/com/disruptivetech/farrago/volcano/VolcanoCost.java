/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2002-2004 Disruptive Tech
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
package com.disruptivetech.farrago.volcano;

import org.eigenbase.relopt.*;


/**
 * <code>VolcanoCost</code> represents the cost of a plan node.
 *
 * <p>
 * This class is immutable: none of the methods (besides {@link #set})
 * modifies any member variables.
 * </p>
 */
class VolcanoCost implements RelOptCost
{
    //~ Static fields/initializers --------------------------------------------

    static final VolcanoCost INFINITY =
        new VolcanoCost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY) {
            public String toString()
            {
                return "{inf}";
            }
        };
    static final VolcanoCost HUGE =
        new VolcanoCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
            public String toString()
            {
                return "{huge}";
            }
        };
    static final VolcanoCost ZERO =
        new VolcanoCost(0.0, 0.0, 0.0) {
            public String toString()
            {
                return "{0}";
            }
        };
    static final VolcanoCost TINY =
        new VolcanoCost(1.0, 1.0, 0.0) {
            public String toString()
            {
                return "{tiny}";
            }
        };


    //~ Instance fields -------------------------------------------------------

    double dCpu;
    double dIo;
    double dRows;

    //~ Constructors ----------------------------------------------------------

    VolcanoCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        set(dRows, dCpu, dIo);
    }

    //~ Methods ---------------------------------------------------------------

    public double getCpu()
    {
        return dCpu;
    }

    public boolean isInfinite()
    {
        return (this == INFINITY) || (this.dRows == Double.POSITIVE_INFINITY)
            || (this.dCpu == Double.POSITIVE_INFINITY)
            || (this.dIo == Double.POSITIVE_INFINITY);
    }

    public double getIo()
    {
        return dIo;
    }

    public boolean isLe(RelOptCost other)
    {
        VolcanoCost that = (VolcanoCost) other;
        return (this == that)
            || ((this.dRows <= that.dRows) && (this.dCpu <= that.dCpu)
            && (this.dIo <= that.dIo));
    }

    public boolean isLt(RelOptCost other)
    {
        return isLe(other) && !equals(other);
    }

    public double getRows()
    {
        return dRows;
    }

    public boolean equals(RelOptCost other)
    {
        if (!(other instanceof VolcanoCost)) {
            return false;
        }
        VolcanoCost that = (VolcanoCost) other;
        return (this == that)
            || ((this.dRows == that.dRows) && (this.dCpu == that.dCpu)
            && (this.dIo == that.dIo));
    }

    public RelOptCost minus(RelOptCost other)
    {
        if (this == INFINITY) {
            return this;
        }
        VolcanoCost that = (VolcanoCost) other;
        return new VolcanoCost(this.dRows - that.dRows, this.dCpu - that.dCpu,
            this.dIo - that.dIo);
    }

    public RelOptCost multiplyBy(double factor)
    {
        if (this == INFINITY) {
            return this;
        }
        return new VolcanoCost(dRows * factor, dCpu * factor, dIo * factor);
    }

    public RelOptCost plus(RelOptCost other)
    {
        VolcanoCost that = (VolcanoCost) other;
        if ((this == INFINITY) || (that == INFINITY)) {
            return INFINITY;
        }
        return new VolcanoCost(this.dRows + that.dRows, this.dCpu + that.dCpu,
            this.dIo + that.dIo);
    }

    public void set(
        double dRows,
        double dCpu,
        double dIo)
    {
        this.dRows = dRows;
        this.dCpu = dCpu;
        this.dIo = dIo;
    }

    public String toString()
    {
        return "{" + dRows + " rows, " + dCpu + " cpu, " + dIo + " io}";
    }
}


// End VolcanoCost.java
