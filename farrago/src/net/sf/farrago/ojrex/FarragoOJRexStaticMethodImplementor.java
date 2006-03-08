/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.lang.reflect.*;
import java.sql.*;

import openjava.ptree.*;
import openjava.mop.*;
import org.eigenbase.rex.*;
import org.eigenbase.reltype.*;

import net.sf.farrago.session.*;

/**
 * FarragoOJRexStaticMethodImplementor implements {@link OJRexImplementor}
 * by generating a call to a static Java method.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexStaticMethodImplementor
    extends FarragoOJRexImplementor
{
    private final Method method;

    private final boolean allowSql;
    
    private final RelDataType returnType;
    
    public FarragoOJRexStaticMethodImplementor(
        Method method,
        boolean allowSql,
        RelDataType returnType)
    {
        this.method = method;
        this.allowSql = allowSql;
        this.returnType = returnType;
    }
    
    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        ExpressionList exprList = new ExpressionList();
        Class [] javaParams = method.getParameterTypes();
        for (int i = 0; i < operands.length; ++i) {
            Expression expr;
            if (javaParams[i].isPrimitive()
                || javaParams[i] == PreparedStatement.class)
            {
                expr = operands[i];
            } else {
                expr = new CastExpression(
                    OJClass.forClass(javaParams[i]),
                    new MethodCall(
                        operands[i],
                        "getNullableData",
                        new ExpressionList()));
            }
            exprList.add(expr);
        }

        Expression callExpr = new MethodCall(
            OJClass.forClass(method.getDeclaringClass()),
            method.getName(),
            exprList);

        String invocationId =
            method.getName()
            + ":"
            + translator.getRelImplementor().generateVariableId();

        FarragoOJRexRelImplementor farragoImplementor =
            (FarragoOJRexRelImplementor) translator.getRelImplementor();
        String serverMofId = farragoImplementor.getServerMofId();

        Expression serverMofIdExpr =
            (serverMofId == null) ? Literal.constantNull()
            : Literal.makeLiteral(serverMofId);

        OJClass ojContextHolderClass =
            OJClass.forClass(FarragoSessionUdrContext.class);
        Variable contextHolder =
            translator.createScratchVariableWithExpression(
                ojContextHolderClass,
                new AllocationExpression(
                    TypeName.forOJClass(ojContextHolderClass),
                    new ExpressionList(
                        Literal.makeLiteral(invocationId),
                        serverMofIdExpr)));

        translator.addStatement(
            new ExpressionStatement(
                new MethodCall(
                    translator.getRelImplementor().getConnectionVariable(),
                    "pushRoutineInvocation",
                    new ExpressionList(
                        contextHolder,
                        Literal.makeLiteral(allowSql)))));

        TryStatement tryStmt = new TryStatement(null, null, null);

        Variable varException =
            translator.getRelImplementor().newVariable();
        tryStmt.setCatchList(
            new CatchList(
                new CatchBlock(
                    new Parameter(
                        TypeName.forOJClass(OJClass.forClass(Throwable.class)),
                        varException.toString()),
                    new StatementList(
                        new ThrowStatement(
                            new MethodCall(
                                translator.getRelImplementor().
                                getConnectionVariable(),
                                "handleRoutineInvocationException",
                                new ExpressionList(
                                    varException,
                                    Literal.makeLiteral(
                                        method.getName()))))))));


        tryStmt.setFinallyBody(
            new StatementList(
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor().
                        getConnectionVariable(),
                        "popRoutineInvocation",
                        new ExpressionList()))));

        if (method.getReturnType() == Void.TYPE) {
            // for a procedure call, the method return is void,
            // so we make up a null value instead
            tryStmt.setBody(
                new StatementList(
                    new ExpressionStatement(callExpr)));
            translator.addStatement(tryStmt);
            if (returnType.isStruct()) {
                // For UDX invocation, we don't want to return
                // anything at all, because it's called from
                // a dead-end thread.
                return null;
            }
            Expression nullVar = translator.createScratchVariable(
                returnType);
            translator.addStatement(
                translator.createSetNullStatement(nullVar, true));
            return nullVar;
        }

        Variable varResult =
            translator.getRelImplementor().newVariable();
        translator.addStatement(
            new VariableDeclaration(
                TypeName.forOJClass(
                    OJClass.forClass(method.getReturnType())),
                new VariableDeclarator(
                    varResult.toString(),
                    null)));

        tryStmt.setBody(
            new StatementList(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varResult,
                        AssignmentExpression.EQUALS,
                        callExpr))));
        translator.addStatement(tryStmt);
        return varResult;
    }
}

// End FarragoOJRexStaticMethodImplementor.java
