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
 * FarragoOJRexNullTestImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for null-test row expressions <code>IS
 * NULL</code> and <code>IS NOT NULL</code>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexNullTestImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private boolean isNull;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexNullTestImplementor(boolean isNull)
    {
        this.isNull = isNull;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        if (call.operands[0].getType().isNullable()) {
            Expression expr =
                new MethodCall(
                    operands[0],
                    NullableValue.NULL_IND_ACCESSOR_NAME,
                    new ExpressionList());
            if (isNull) {
                return expr;
            } else {
                return new UnaryExpression(UnaryExpression.NOT, expr);
            }
        } else {
            if (isNull) {
                return Literal.constantFalse();
            } else {
                return Literal.constantTrue();
            }
        }
    }
}

// End FarragoOJRexNullTestImplementor.java
