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

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * This implementor writes code to retrieve the next value from a sequence.
 *
 * @author John Pham
 * @version $Id$
 */
class FarragoOJRexNextValueImplementor
    extends FarragoOJRexImplementor
{
    //~ Static fields/initializers ---------------------------------------------

    private static String GET_SEQUENCE_METHOD_NAME = "getSequenceAccessor";

    //~ Methods ----------------------------------------------------------------

    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // make sequence a member variable to avoid retrieving it every
        // iteration
        Variable sequence = translator.newVariable();
        FieldDeclaration declaration =
            translator.newMember(
                ModifierList.PRIVATE,
                OJClass.forClass(FarragoSequenceAccessor.class),
                sequence,
                null);
        translator.addMember(declaration);

        // before processing a row, inialize the sequence if
        // it has not been initialized yet; no need to synchronize because
        // member is non-static
        Expression mofId = translator.toString(operands[0]);
        Expression expForSequence =
            new MethodCall(
                translator.getRelImplementor().getConnectionVariable(),
                GET_SEQUENCE_METHOD_NAME,
                new ExpressionList(mofId));
        Statement stmt = translator.setIfNull(sequence, expForSequence);
        translator.addStatement(stmt);

        // perform value access once per row; a variable is returned
        // to avoid recomputation of the expression
        OJClass ojClass = translator.typeToOJClass(call.getType());
        Variable value = translator.newVariable();
        Expression expForValue =
            new MethodCall(
                sequence,
                FarragoSequenceAccessor.NEXT_VALUE_METHOD_NAME,
                new ExpressionList());
        stmt = translator.declareLocalVariable(ojClass, value, expForValue);
        translator.addStatement(stmt);

        return value;
    }
}

// End FarragoOJRexNextValueImplementor.java
