/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.ojrex;

import net.sf.farrago.type.runtime.*;

import openjava.ptree.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * FarragoOJRexTruthTestImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for truth-test row expressions IS TRUE and IS FALSE.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexTruthTestImplementor extends FarragoOJRexImplementor
{
    //~ Instance fields -------------------------------------------------------

    private boolean isTrue;

    //~ Constructors ----------------------------------------------------------

    public FarragoOJRexTruthTestImplementor(boolean isTrue)
    {
        this.isTrue = isTrue;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        Expression operand = operands[0];
        if (call.operands[0].getType().isNullable()) {
            Expression nonNull =
                new FieldAccess(operand, NullablePrimitive.VALUE_FIELD_NAME);
            nonNull = maybeNegate(nonNull);

            return new BinaryExpression(
                new UnaryExpression(
                    UnaryExpression.NOT,
                    new MethodCall(
                        operand,
                        NullableValue.NULL_IND_ACCESSOR_NAME,
                        new ExpressionList())),
                BinaryExpression.LOGICAL_AND,
                nonNull);
        } else {
            return maybeNegate(operand);
        }
    }

    private Expression maybeNegate(Expression expr)
    {
        if (isTrue) {
            return expr;
        } else {
            return new UnaryExpression(UnaryExpression.NOT, expr);
        }
    }
}


// End FarragoOJRexTruthTestImplementor.java
