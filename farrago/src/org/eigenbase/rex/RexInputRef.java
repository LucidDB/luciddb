/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;


/**
 * Variable which references a field of an input relational expression.
 *
 * <p>Fields of the input are 0-based. If there is more than one input, they
 * are numbered consecutively. For example, if the inputs to a join are<ul>
 *
 * <li>Input #0: EMP(EMPNO, ENAME, DEPTNO) and</li>
 * <li>Input #1: DEPT(DEPTNO AS DEPTNO2, DNAME)</li>
 *
 * </ul>then the fields are:<ul>
 * <li>Field #0: EMPNO</li>
 * <li>Field #1: ENAME</li>
 * <li>Field #2: DEPTNO (from EMP)</li>
 * <li>Field #3: DEPTNO2 (from DEPT)</li>
 * <li>Field #4: DNAME</li>
 *
 * </ul>So <code>RexInputRef(3,Integer)</code> is the correct reference
 * for the field DEPTNO2.</p>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexInputRef extends RexVariable
{
    //~ Instance fields -------------------------------------------------------

    public final int index;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an input variable.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     */
    public RexInputRef(
        int index,
        RelDataType type)
    {
        super("$" + index, type);
        Util.pre(type != null, "type != null");
        this.index = index;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new RexInputRef(index, type);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitInputRef(this);
    }
}


// End RexInputRef.java
