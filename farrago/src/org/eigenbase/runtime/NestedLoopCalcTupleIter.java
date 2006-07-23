/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.runtime;

/**
 * <code>NestedLoopCalcTupleIter</code> is a specialization of {@link
 * CalcTupleIter} for use in implementing nested loop inner joins over
 * iterators.
 *
 * <p>REVIEW jvs 20-Mar-2004: I have parameterized this to handle inner and left
 * outer joins, as well as one-to-many and many-to-one variants. This comes at
 * the price of some efficiency. It would probably be better to write
 * specialized bases for each purpose.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class NestedLoopCalcTupleIter
    extends CalcTupleIter
{

    //~ Instance fields --------------------------------------------------------

    protected Object rightIterator;
    protected Object leftObj;
    protected Object rightObj;
    private boolean isOpen;
    private boolean isLeftOuter;
    private boolean needNullRow;

    //~ Constructors -----------------------------------------------------------

    protected NestedLoopCalcTupleIter(
        TupleIter leftIterator,
        boolean isLeftOuter)
    {
        super(leftIterator);
        this.isLeftOuter = isLeftOuter;
    }

    //~ Methods ----------------------------------------------------------------

    // implement TupleIter
    public Object fetchNext()
    {
        if (!isOpen) {
            isOpen = true;
            open();
        }
        for (;;) {
            if (leftObj == null) {
                Object next = inputIterator.fetchNext();
                if (next instanceof NoDataReason) {
                    return next;
                }

                leftObj = next;
            }
            if (rightIterator == null) {
                rightIterator = getNextRightIterator();
                needNullRow = isLeftOuter;
            }
            if (rightIterator instanceof TupleIter) {
                TupleIter ri = (TupleIter) rightIterator;
                Object next = ri.fetchNext();
                if (next == NoDataReason.END_OF_DATA) {
                    if (needNullRow) {
                        needNullRow = false;
                        return calcRightNullRow();
                    }
                    leftObj = null;
                    rightObj = null;
                    rightIterator = null;
                    continue;
                }
                rightObj = next;
            } else {
                rightObj = rightIterator;
                rightIterator = TupleIter.EMPTY_ITERATOR;
            }
            Object row = calcJoinRow();
            if (row != null) {
                needNullRow = false;
                return row;
            }
        }
    }

    /**
     * Method which can be overridden by subclasses to carry out
     * post-constructor initialization.
     */
    protected void open()
    {
    }

    /**
     * Method to be implemented by subclasses to determine next right-hand
     * iterator based on current value of leftObj. For a many-to-one join, this
     * can return the right-hand object directly instead of a TupleIter, but
     * should return {@link TupleIter#EMPTY_ITERATOR} for a mismatch.
     *
     * @return iterator or object
     */
    protected abstract Object getNextRightIterator();

    /**
     * Method to be implemented by subclasses to either calculate the next
     * joined row based on current values of leftObj and rightObj, or else to
     * filter out this combination.
     *
     * @return row or null for filtered oute
     */
    protected abstract Object calcJoinRow();

    /**
     * Method to be implemented by subclasses to calculate a mismatch row in a
     * left outer join. Inner joins can use the default (return null) because it
     * will never be called.
     *
     * @return row with all right fields set to null
     */
    protected Object calcRightNullRow()
    {
        return null;
    }
}

// End NestedLoopCalcTupleIter.java
