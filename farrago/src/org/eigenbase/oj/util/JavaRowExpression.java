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
package org.eigenbase.oj.util;

import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.reltype.RelDataType;
import openjava.ptree.Expression;
import openjava.ptree.Literal;
import openjava.mop.Environment;

/**
 * A row expression which is implemented by an underlying Java expression.
 *
 * <p>This is a leaf node of a {@link RexNode} tree, but the Java expression,
 * represented by a {@link Expression} object, may be complex.</p>
 *
 * @see JavaRexBuilder
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class JavaRowExpression extends RexNode {
    final Environment env;
    private final RelDataType type;
    public final Expression expression;

    public JavaRowExpression(Environment env, RelDataType type,
            Expression expression) {
        this.env = env;
        this.type = type;
        this.expression = expression;
        this.digest = "Java(" + expression + ")";
    }

    public boolean isAlwaysTrue() {
        return expression == Literal.constantTrue();
    }

    public void accept(RexVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    public RelDataType getType() {
        return type;
    }

    public Object clone() {
        return new JavaRowExpression(env, type, expression);
    }
}

// End JavaRowExpression.java
