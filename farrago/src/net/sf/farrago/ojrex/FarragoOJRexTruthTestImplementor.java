/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.ojrex;

import net.sf.farrago.type.runtime.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.rex.*;

import openjava.ptree.*;

/**
 * FarragoOJRexTruthTestImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for truth-test row expressions IS TRUE and IS FALSE.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexTruthTestImplementor extends FarragoOJRexImplementor
{
    private boolean isTrue;
    
    public FarragoOJRexTruthTestImplementor(boolean isTrue)
    {
        this.isTrue = isTrue;
    }
    
    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,RexCall call,Expression [] operands)
    {
        Expression operand = operands[0];
        if (call.operands[0].getType().isNullable()) {
            Expression nonNull =
                new FieldAccess(operand,NullablePrimitive.VALUE_FIELD_NAME);
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
            return new UnaryExpression(UnaryExpression.NOT,expr);
        }
    }
}

// End FarragoOJRexTruthTestImplementor.java
