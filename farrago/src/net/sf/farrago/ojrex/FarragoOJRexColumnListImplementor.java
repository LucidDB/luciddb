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

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * FarragoOJRexColumnListImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for a column-list constructor.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoOJRexColumnListImplementor
    extends FarragoOJRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // allocate an OJClass object corresponding to a Java list and declare
        // a variable for that class
        OJClass listClass = OJClass.forClass(java.util.ArrayList.class);
        Variable variable =
            translator.createScratchVariableWithExpression(
                listClass,
                new AllocationExpression(
                    TypeName.forOJClass(listClass),
                    null,
                    null));

        // generate calls to add the individual column names to the list
        // object
        RexNode [] columns = (RexNode []) call.getOperands();
        for (int i = 0; i < columns.length; i++) {
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        variable,
                        "add",
                        new ExpressionList(
                            Literal.makeLiteral(
                                RexLiteral.stringValue(columns[i]))))));
        }

        return variable;
    }
}

// End FarragoOJRexColumnListImplementor.java
