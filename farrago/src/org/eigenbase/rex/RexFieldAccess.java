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
import org.eigenbase.reltype.RelDataTypeField;


/**
 * Access to a field of a row-expression.
 *
 * <p>You might expect to use a <code>RexFieldAccess</code> to access columns
 * of relational tables, for example, the expression <code>emp.empno</code> in
 * the query
 *
 * <blockquote><pre>SELECT emp.empno FROM emp</pre></blockquote>
 *
 * but there is a specialized expression {@link RexInputRef} for this
 * purpose. So in practice, <code>RexFieldAccess</code> is usually used to
 * access fields of correlating variabless, for example the expression
 * <code>emp.deptno</code> in
 *
 * <blockquote><pre>SELECT ename
 * FROM dept
 * WHERE EXISTS (
 *     SELECT NULL
 *     FROM emp
 *     WHERE emp.deptno = dept.deptno
 *     AND gender = 'F')</pre>
 * </blockquote>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexFieldAccess extends RexNode
{
    //~ Instance fields -------------------------------------------------------

    private RexNode expr;
    private final RelDataTypeField field;

    //~ Constructors ----------------------------------------------------------

    RexFieldAccess(
        RexNode expr,
        RelDataTypeField field)
    {
        this.expr = expr;
        this.field = field;
        computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataTypeField getField()
    {
        return field;
    }

    public RelDataType getType()
    {
        return field.getType();
    }

    public Object clone()
    {
        return new RexFieldAccess(expr, field);
    }

    public RexKind getKind()
    {
        return RexKind.FieldAccess;
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitFieldAccess(this);
    }

    public RexNode accept(RexShuttle shuttle)
    {
        return shuttle.visitFieldAccess(this);
    }

    /**
     * Returns the expression whose field is being accessed.
     */
    public RexNode getReferenceExpr()
    {
        return expr;
    }

    public void setReferenceExpr(RexNode expr)
    {
        this.expr = expr;
    }

    /**
     * Returns the name of the field.
     */
    public String getName()
    {
        return field.getName();
    }

    public String toString()
    {
        return computeDigest();
    }

    private String computeDigest()
    {
        return (this.digest = expr + "." + field.getName());
    }
}


// End RexFieldAccess.java
