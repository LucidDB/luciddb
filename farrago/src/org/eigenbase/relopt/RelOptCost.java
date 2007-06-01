/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
 * RelOptCost defines an interface for optimizer cost in terms of number of rows
 * processed, CPU cost, and I/O cost. Optimizer implementations may use all of
 * this information, or selectively ignore portions of it. The specific units
 * for all of these quantities are rather vague; most relational expressions
 * provide a default cost calculation, but optimizers can override this by
 * plugging in their own cost models with well-defined meanings for each unit.
 * Optimizers which supply their own cost models may also extend this interface
 * with additional cost metrics such as memory usage.
 */
public interface RelOptCost
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return number of rows processed; this should not be confused with the
     * row count produced by a relational expression ({@link
     * org.eigenbase.rel.RelNode#getRows})
     */
    public double getRows();

    /**
     * @return usage of CPU resources
     */
    public double getCpu();

    /**
     * @return usage of I/O resources
     */
    public double getIo();

    /**
     * @return true iff this cost represents an expression that hasn't actually
     * been implemented (e.g. a pure relational algebra expression) or can't
     * actually be implemented, e.g. a transfer of data between two disconnected
     * sites
     */
    public boolean isInfinite();

    // REVIEW jvs 3-Apr-2006:  we should standardize this
    // to Comparator/equals/hashCode
    /**
     * Compares this to another cost.
     *
     * @param cost another cost
     *
     * @return true iff this is exactly equal to other cost
     */
    public boolean equals(RelOptCost cost);

    /**
     * Compares this to another cost.
     *
     * @param cost another cost
     *
     * @return true iff this is less than or equal to other cost
     */
    public boolean isLe(RelOptCost cost);

    /**
     * Compares this to another cost.
     *
     * @param cost another cost
     *
     * @return true iff this is strictly less than other cost
     */
    public boolean isLt(RelOptCost cost);

    /**
     * Adds another cost to this.
     *
     * @param cost another cost
     *
     * @return sum of this and other cost
     */
    public RelOptCost plus(RelOptCost cost);

    /**
     * Subtracts another cost from this.
     *
     * @param cost another cost
     *
     * @return difference between this and other cost
     */
    public RelOptCost minus(RelOptCost cost);

    /**
     * Multiplies this cost by a scalar factor.
     *
     * @param factor scalar factor
     *
     * @return scalar product of this and factor
     */
    public RelOptCost multiplyBy(double factor);

    /**
     * Forces implementations to override {@link Object#toString} and provide a
     * good cost rendering to use during tracing.
     */
    public String toString();
}

// End RelOptCost.java
