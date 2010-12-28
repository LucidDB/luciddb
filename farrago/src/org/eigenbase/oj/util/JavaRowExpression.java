/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.oj.util;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A row expression which is implemented by an underlying Java expression.
 *
 * <p>This is a leaf node of a {@link RexNode} tree, but the Java expression,
 * represented by a {@link Expression} object, may be complex.</p>
 *
 * @author jhyde
 * @version $Id$
 * @see JavaRexBuilder
 * @since Nov 23, 2003
 */
public class JavaRowExpression
    extends RexNode
{
    //~ Instance fields --------------------------------------------------------

    final Environment env;
    private final RelDataType type;
    private final Expression expression;

    //~ Constructors -----------------------------------------------------------

    public JavaRowExpression(
        Environment env,
        RelDataType type,
        Expression expression)
    {
        this.env = env;
        this.type = type;
        this.expression = expression;
        this.digest = "Java(" + expression + ")";
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isAlwaysTrue()
    {
        return expression == Literal.constantTrue();
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        if (visitor instanceof RexShuttle) {
            return (R) this;
        }
        return null;
    }

    public RelDataType getType()
    {
        return type;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public JavaRowExpression clone()
    {
        return new JavaRowExpression(env, type, expression);
    }
}

// End JavaRowExpression.java
