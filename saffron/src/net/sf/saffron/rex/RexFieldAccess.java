/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.rex;

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;

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
public class RexFieldAccess extends RexNode {
    public RexNode expr;
    private final SaffronField field;

    RexFieldAccess(RexNode expr,SaffronField field) {
        this.expr = expr;
        this.field = field;
        computeDigest();
    }

    public SaffronField getField() {
        return field;
    }
    
    public SaffronType getType() {
        return field.getType();
    }

    public Object clone() {
        return new RexFieldAccess(expr, field);
    }

    public RexKind getKind() {
        return RexKind.FieldAccess;
    }

    public void accept(RexVisitor visitor) {
        visitor.visitFieldAccess(this);
    }

    /**
     * Returns the expression whose field is being accessed.
     */ 
    public RexNode getReferenceExpr() {
        return expr;
    }

    /**
     * Returns the name of the field.
     */
    public String getName() {
        return field.getName();
    }

    public String toString() {
        return computeDigest();
    }

    private String computeDigest() {
        return (this.digest = expr + "." + field.getName());
    }
}

// End RexFieldAccess.java
