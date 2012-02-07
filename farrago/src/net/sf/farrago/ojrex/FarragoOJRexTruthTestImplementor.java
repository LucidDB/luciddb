/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.ojrex;

import net.sf.farrago.type.runtime.*;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * FarragoOJRexTruthTestImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for truth-test row expressions <code>
 * IS TRUE</code> and <code>IS FALSE</code>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexTruthTestImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final boolean isTrue;
    private final boolean negated;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexTruthTestImplementor(boolean isTrue, boolean negated)
    {
        this.isTrue = isTrue;
        this.negated = negated;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // Expression     negated isTrue Implementation
        // ============== ======= ====== ===================
        // x is not true  true    true   !x.val || x.isnull
        // x is true      false   true   x.val && !x.isnull
        // x is not false true    false  x.val || x.isnull
        // x is false     false   false  !x.val && !x.isnull
        Expression operand = operands[0];
        if (call.operands[0].getType().isNullable()) {
            Expression val =
                new FieldAccess(operand, NullablePrimitive.VALUE_FIELD_NAME);
            final MethodCall isnull =
                new MethodCall(
                    operand,
                    NullableValue.NULL_IND_ACCESSOR_NAME,
                    new ExpressionList());
            return new BinaryExpression(
                maybeNegate(
                    isnull,
                    !negated),
                negated ? BinaryExpression.LOGICAL_OR
                : BinaryExpression.LOGICAL_AND,
                maybeNegate(
                    val,
                    negated == isTrue));
        } else {
            return maybeNegate(operand, isTrue == negated);
        }
    }

    private Expression maybeNegate(Expression expr, boolean negate)
    {
        if (negate) {
            return new UnaryExpression(UnaryExpression.NOT, expr);
        } else {
            return expr;
        }
    }
}

// End FarragoOJRexTruthTestImplementor.java
