/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
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
package net.sf.farrago.ojrex;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexUnaryExpressionImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for row expressions which can be
 * translated to instances of OpenJava {@link UnaryExpression}.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class FarragoOJRexUnaryExpressionImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final int ojUnaryExpressionOrdinal;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexUnaryExpressionImplementor(
        int ojUnaryExpressionOrdinal)
    {
        this.ojUnaryExpressionOrdinal = ojUnaryExpressionOrdinal;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // TODO:  overflow detection, type promotion, etc.  Also, if global
        // analysis is used on the expression, we can reduce the number of
        // null-tests.
        Expression [] valueOperands = new Expression[1];

        for (int i = 0; i < 1; ++i) {
            valueOperands[i] =
                translator.convertPrimitiveAccess(
                    operands[i],
                    call.operands[i]);
        }

        if (!call.getType().isNullable()) {
            return implementNotNull(translator, call, valueOperands);
        }

        Variable varResult = translator.createScratchVariable(call.getType());

        Expression nullTest = null;
        nullTest =
            translator.createNullTest(
                call.operands[0],
                operands[0],
                nullTest);
        assert nullTest != null;

        // TODO:  generalize to stuff other than NullablePrimitive
        Statement assignmentStmt =
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(
                        varResult,
                        NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        Statement ifStatement =
            new IfStatement(
                nullTest,
                new StatementList(
                    translator.createSetNullStatement(
                        varResult,
                        true)),
                new StatementList(
                    translator.createSetNullStatement(
                        varResult,
                        false),
                    assignmentStmt));

        translator.addStatement(ifStatement);

        return varResult;
    }

    private Expression implementNotNull(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType returnType = call.getType();

        FarragoTypeFactory factory = translator.getFarragoTypeFactory();
        Expression expr =
            new UnaryExpression(
                operands[0],
                ojUnaryExpressionOrdinal);
        if ((returnType.getSqlTypeName() != SqlTypeName.BOOLEAN)
            && (factory.getClassForPrimitive(returnType) != null))
        {
            // Cast to correct primitive return type so compiler is happy
            return new CastExpression(
                OJClass.forClass(factory.getClassForPrimitive(returnType)),
                expr);
        } else {
            return expr;
        }
    }
}

// End FarragoOJRexUnaryExpressionImplementor.java
