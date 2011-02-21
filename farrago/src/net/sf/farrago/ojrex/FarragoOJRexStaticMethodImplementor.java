/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.session.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FarragoOJRexStaticMethodImplementor implements {@link
 * org.eigenbase.oj.rex.OJRexImplementor} by generating a call to a static Java
 * method.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexStaticMethodImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final Method method;

    private final boolean allowSql;

    private final RelDataType returnType;

    private final Class declaringClass;

    private final String methodName;

    private String impersonatedUser;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an implementor for a call to a user-defined routine (UDR)
     * implemented as an external Java static method.
     *
     * @param method UDR implementation
     * @param allowSql whether to allow reentrant invocation of SQL from within
     * the called method
     * @param returnType SQL type to impose on returned Java value
     */
    public FarragoOJRexStaticMethodImplementor(
        Method method,
        boolean allowSql,
        RelDataType returnType)
    {
        assert (Modifier.isStatic(method.getModifiers()));

        this.method = method;
        this.allowSql = allowSql;
        this.returnType = returnType;
        this.declaringClass = method.getDeclaringClass();
        this.methodName = method.getName();
    }

    /**
     * Creates an implementor for a call to a system-provided static method.
     *
     * @param declaringClass class which declares method
     * @param methodName name of static method; overload resolution happens
     * implicitly based on operand types in each invocation
     */
    public FarragoOJRexStaticMethodImplementor(
        Class declaringClass,
        String methodName)
    {
        this.method = null;
        this.allowSql = false;
        this.returnType = null;
        this.declaringClass = declaringClass;
        this.methodName = methodName;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets a user to impersonate during execution.
     *
     * @param impersonatedUser user to impersonate during execution,
     * or null for no impersonation
     */
    public void setImpersonatedUser(String impersonatedUser)
    {
        this.impersonatedUser = impersonatedUser;
    }

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        if (method == null) {
            // system-defined
            return implementSystemCall(translator, call, operands);
        }

        ExpressionList exprList = new ExpressionList();
        Class [] javaParams = method.getParameterTypes();
        for (int i = 0; i < operands.length; ++i) {
            Expression expr;
            if (javaParams[i].isPrimitive()
                || (javaParams[i] == ResultSet.class)
                || ((javaParams[i] == PreparedStatement.class)
                    || (javaParams[i] == java.util.List.class)))
            {
                expr = operands[i];
            } else {
                expr =
                    new CastExpression(
                        OJClass.forClass(javaParams[i]),
                        new MethodCall(
                            operands[i],
                            "getNullableData",
                            new ExpressionList()));
            }
            exprList.add(expr);
        }

        Expression callExpr =
            new MethodCall(
                OJClass.forClass(declaringClass),
                methodName,
                exprList);

        String invocationId =
            methodName
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

        Expression impersonatedUserExpr;
        if (impersonatedUser == null) {
            impersonatedUserExpr = Literal.constantNull();
        } else {
            impersonatedUserExpr = Literal.makeLiteral(impersonatedUser);
        }
        translator.addStatement(
            new ExpressionStatement(
                new MethodCall(
                    translator.getRelImplementor().getConnectionVariable(),
                    "pushRoutineInvocation",
                    new ExpressionList(
                        contextHolder,
                        Literal.makeLiteral(allowSql),
                        impersonatedUserExpr))));

        TryStatement tryStmt = new TryStatement(null, null, null);

        Variable varException = translator.getRelImplementor().newVariable();
        tryStmt.setCatchList(
            new CatchList(
                new CatchBlock(
                    new Parameter(
                        TypeName.forOJClass(OJClass.forClass(
                                Throwable.class)),
                        varException.toString()),
                    new StatementList(
                        new ThrowStatement(
                            new MethodCall(
                                translator.getRelImplementor()
                                          .getConnectionVariable(),
                                "handleRoutineInvocationException",
                                new ExpressionList(
                                    varException,
                                    Literal.makeLiteral(
                                        methodName))))))));

        tryStmt.setFinallyBody(
            new StatementList(
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor().getConnectionVariable(),
                        "popRoutineInvocation",
                        new ExpressionList()))));

        if (method.getReturnType() == Void.TYPE) {
            // for a procedure call, the method return is void,
            // so we make up the value 0 instead; since we're
            // treating procedure invocation as DML, this will
            // appear to the client as 0 rows processed
            tryStmt.setBody(
                new StatementList(new ExpressionStatement(callExpr)));
            translator.addStatement(tryStmt);
            if (returnType.isStruct()) {
                // For UDX invocation, we don't want to return
                // anything at all, because it's called from
                // a dead-end thread.
                return null;
            }
            return Literal.makeLiteral((long) 0);
        }

        Variable varResult = translator.getRelImplementor().newVariable();
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

    private Expression implementSystemCall(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        ExpressionList exprList = new ExpressionList();
        for (int i = 0; i < operands.length; ++i) {
            exprList.add(operands[i]);
        }
        return new MethodCall(
            OJClass.forClass(declaringClass),
            methodName,
            exprList);
    }
}

// End FarragoOJRexStaticMethodImplementor.java
