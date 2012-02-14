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

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FarragoOJRexRowImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for <code>ROW</code> and UDT
 * constructors.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexRowImplementor
    extends FarragoOJRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // REVIEW jvs 16-Oct-2006: We currently generate code with a helper
        // method for each value; this keeps us conservative about the total
        // method bytecode 32K limit.  Should probably refactor together with
        // IterCalcRel to do properly balanced decomposition.

        RelDataType rowType = call.getType();
        Variable variable = translator.createScratchVariable(rowType);
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < operands.length; ++i) {
            final FarragoRexToOJTranslator.Frame methodBodyFrame =
                translator.getSubFrame(i);
            final StatementList methodBody = methodBodyFrame.stmtList;
            final RelDataTypeField field = fields[i];
            translator.convertCastOrAssignmentWithStmtList(
                methodBody,
                translator.getRepos().getLocalizedObjectName(
                    fields[i].getName()),
                fields[i].getType(),
                call.operands[i].getType(),
                translator.convertFieldAccess(variable, field),
                operands[i]);

            String methodName = "calc_" + variable.toString() + "_f_" + i;
            MemberDeclaration methodDecl =
                new MethodDeclaration(
                    new ModifierList(
                        ModifierList.PRIVATE | ModifierList.FINAL),
                    TypeName.forOJClass(OJSystem.VOID),
                    methodName,
                    new ParameterList(),
                    null,
                    methodBody);
            translator.addMember(methodDecl);

            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        methodName,
                        new ExpressionList())));
        }
        return variable;
    }
}

// End FarragoOJRexRowImplementor.java
