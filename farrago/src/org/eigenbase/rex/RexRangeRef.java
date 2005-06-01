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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;


/**
 * Reference to a range of columns.
 *
 * <p>This construct is used only during the process of translating a
 * {@link org.eigenbase.sql.SqlNode SQL} tree to a
 * {@link org.eigenbase.rel.RelNode rel}/{@link RexNode rex} tree.
 * <em>Regular {@link RexNode rex} trees do not contain this
 * construct.</em></p>
 *
 * <p>While translating a join of
 * EMP(EMPNO, ENAME, DEPTNO) to DEPT(DEPTNO2, DNAME) we create
 * <code>RexRangeRef(DeptType,3)</code> to represent the pair of
 * columns (DEPTNO2, DNAME) which came from DEPT. The type has 2 columns,
 * and therefore the range represents columns {3, 4} of the input.</p>
 *
 * <p>Suppose we later create
 * a reference to the DNAME field of this RexRangeRef; it will
 * return a <code>{@link RexInputRef}(5,Integer)</code>, and the
 * {@link org.eigenbase.rex.RexRangeRef} will disappear.</p>
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 */
public class RexRangeRef extends RexNode
{
    //~ Instance fields -------------------------------------------------------

    private final RelDataType type;
    private final int offset;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a range reference.
     * @param rangeType   Type of the record returned
     * @param offset Offset of the first column within the input record
     */
    RexRangeRef(
        RelDataType rangeType,
        int offset)
    {
        this.type = rangeType;
        this.offset = offset;
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType getType()
    {
        return type;
    }

    public int getOffset()
    {
        return offset;
    }

    public Object clone()
    {
        return new RexRangeRef(type, offset);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitRangeRef(this);
    }
}


// End RexRangeRef.java
