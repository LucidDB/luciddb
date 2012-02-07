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

import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * An {@link OJRexImplementor} for reinterpret casts. Reinterpret casts are
 * important for generated Java code, because while Sql types may share the same
 * primitive types, they may require different wrappers. Currently this class
 * can only handle conversions between Decimals and Bigints.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoOJRexReinterpretImplementor
    extends FarragoOJRexImplementor
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs an OJRexReinterpretCastImplementor
     */
    public FarragoOJRexReinterpretImplementor()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return call.isA(RexKind.Reinterpret);
    }

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        Util.pre(
            call.isA(RexKind.Reinterpret),
            "call.isA(RexKind.Reinterpret)");

        RelDataType retType = call.getType();
        Expression retVal = null;
        if (SqlTypeUtil.isDecimal(retType) || SqlTypeUtil.isInterval(retType)) {
            // cast long to decimal
            Variable varResult = translator.createScratchVariable(retType);
            ExpressionList args;
            if (operands.length == 1) {
                args = new ExpressionList(operands[0]);
            } else {
                assert operands.length == 2;
                args = new ExpressionList(operands[0], operands[1]);
            }
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        varResult,
                        EncodedSqlDecimal.REINTERPRET_METHOD_NAME,
                        args)));
            retVal = varResult;
        } else if (SqlTypeUtil.isExactNumeric(retType)) {
            Expression source = operands[0];
            // This is to handle case where source has already been cast to a
            // long by FarragoOJRexBinaryExpressionImplementor or
            // FarragoOJRexUnaryExpressionImplementor. This feels like two
            // different patterns are being used to maintain type consistency
            // meet at this point.
            if (SqlTypeUtil.isInterval(call.getOperands()[0].getType())) {
                OJClass retTypeOjClass =
                    OJUtil.typeToOJClass(
                        retType,
                        translator.getFarragoTypeFactory());
                OJClass clazz = null;
                try {
                    clazz = source.getType(retTypeOjClass.getEnvironment());
                } catch (Exception e) {
                }
                if (retTypeOjClass.equals(clazz)) {
                    return source;
                }
            }

            if (retType.isNullable()) {
                // cast decimal to nullable long
                Variable varResult = translator.createScratchVariable(retType);
                translator.addStatement(
                    new ExpressionStatement(
                        new MethodCall(
                            source,
                            EncodedSqlDecimal.ASSIGN_TO_METHOD_NAME,
                            new ExpressionList(varResult))));
                retVal = varResult;
            } else {
                // cast decimal to non null long
                retVal =
                    new FieldAccess(
                        source,
                        EncodedSqlDecimal.VALUE_FIELD_NAME);
            }
        } else {
            assert false;
        }
        checkNullability(
            translator,
            retType,
            call.operands[0],
            operands[0]);
        return retVal;
    }

    /**
     * Inserts a null test if a value is nullable, but a target type is not.
     */
    private void checkNullability(
        FarragoRexToOJTranslator translator,
        RelDataType targetType,
        RexNode rexValue,
        Expression javaValue)
    {
        ExpressionStatement nullTest = null;
        if (!targetType.isNullable() && rexValue.getType().isNullable()) {
            nullTest =
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor().getConnectionVariable(),
                        "checkNotNull",
                        new ExpressionList(
                            Literal.makeLiteral(rexValue.toString()),
                            javaValue)));
            translator.addStatement(nullTest);
        }
    }
}

// End FarragoOJRexReinterpretImplementor.java
