/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.runtime;

import java.util.*;

/**
 * <code>NestedLoopCalcIterator</code> is a specialization of {@link
 * CalcIterator} for use in implementing nested loop inner joins
 * over iterators.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class NestedLoopCalcIterator extends CalcIterator
{
    protected Iterator rightIterator;
    
    protected Object leftObj;

    protected Object rightObj;

    private boolean isOpen;

    protected NestedLoopCalcIterator(Iterator leftIterator)
    {
        super(leftIterator);
    }
    
    // override CalcIterator
    public void remove()
    {
        rightIterator.remove();
        super.remove();
    }

    // implement CalcIterator
    protected Object calcNext()
    {
        if (!isOpen) {
            isOpen = true;
            open();
        }
        for (;;) {
            if (leftObj == null) {
                if (!inputIterator.hasNext()) {
                    return null;
                }
                leftObj = inputIterator.next();
            }
            if (rightIterator == null) {
                rightIterator = getNextRightIterator();
            }
            if (!rightIterator.hasNext()) {
                leftObj = null;
                rightObj = null;
                rightIterator = null;
                continue;
            }
            rightObj = rightIterator.next();
            Object row = calcJoinRow();
            if (row != null) {
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
     * iterator based on current value of leftObj.
     *
     * @return iterator (never null)
     */
    protected abstract Iterator getNextRightIterator();

    /**
     * Method to be implemented by subclasses to either calculate the next
     * joined row based on current values of leftObj and rightObj, or else to
     * filter out this combination.
     *
     * @return row or null for filtered oute
     */
    protected abstract Object calcJoinRow();
}

// End NestedLoopCalcIterator.java
