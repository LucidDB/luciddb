/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package org.eigenbase.rex;

import org.eigenbase.reltype.*;


/**
 * Variable which references a field of an input relational expression.
 *
 * <p>Fields of the input are 0-based. If there is more than one input, they are
 * numbered consecutively. For example, if the inputs to a join are
 *
 * <ul>
 * <li>Input #0: EMP(EMPNO, ENAME, DEPTNO) and</li>
 * <li>Input #1: DEPT(DEPTNO AS DEPTNO2, DNAME)</li>
 * </ul>
 *
 * then the fields are:
 *
 * <ul>
 * <li>Field #0: EMPNO</li>
 * <li>Field #1: ENAME</li>
 * <li>Field #2: DEPTNO (from EMP)</li>
 * <li>Field #3: DEPTNO2 (from DEPT)</li>
 * <li>Field #4: DNAME</li>
 * </ul>
 *
 * So <code>RexInputRef(3,Integer)</code> is the correct reference for the field
 * DEPTNO2.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public class RexInputRef
    extends RexSlot
{
    //~ Static fields/initializers ---------------------------------------------

    // array of common names, to reduce memory allocations
    private static final String [] names = makeArray(32, "$");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an input variable.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     *
     * @pre type != null
     * @pre index >= 0
     */
    public RexInputRef(
        int index,
        RelDataType type)
    {
        super(
            createName(index),
            index,
            type);
    }

    //~ Methods ----------------------------------------------------------------

    public RexInputRef clone()
    {
        return new RexInputRef(index, type);
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        return visitor.visitInputRef(this);
    }

    /**
     * Creates a name for an input reference, of the form "$index". If the index
     * is low, uses a cache of common names, to reduce gc.
     */
    public static String createName(int index)
    {
        return (index < names.length) ? names[index] : ("$" + index);
    }
}

// End RexInputRef.java
