/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.oj.util;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.Util;
import net.sf.saffron.sql.SqlOperator;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.util.List;
import java.math.BigInteger;

/**
 * Extends {@link RexBuilder} to builds row-expressions including those
 * involving Java code.
 *
 * @see JavaRowExpression
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class JavaRexBuilder extends RexBuilder {
    OJTranslator translator = new OJTranslator();

    public JavaRexBuilder(SaffronTypeFactory typeFactory) {
        super(typeFactory);
    }

    public RexNode makeFieldAccess(RexNode exp, String fieldName) {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression jexp = (JavaRowExpression) exp;
            final FieldAccess fieldAccess = new FieldAccess(jexp.expression,
                    fieldName);
            return makeJava(jexp.env, fieldAccess);
        } else {
            return super.makeFieldAccess(exp, fieldName);
        }
    }

    /**
     * Creates a call to a Java method.
     * @param exp        Target of the method
     * @param methodName Name of the method
     * @param args       Argument expressions; null means no arguments
     * @return Method call
     */
    public RexNode createMethodCall(Environment env, RexNode exp,
            String methodName, List args) {
        ExpressionList ojArgs = translator.toJava(args);
        Expression ojExp = translator.toJava(exp);
        return makeJava(env, new MethodCall(ojExp, methodName, ojArgs));
    }

    public RexNode makeJava(Environment env, Expression expr) {
        final OJClass ojClass;
        try {
            ojClass = expr.getType(env);
        } catch (Exception e) {
            throw Util.newInternal(e, "Error deriving type of expression " +
                    expr);
        }
        SaffronType type = OJUtil.ojToType(this.typeFactory, ojClass);
        return new JavaRowExpression(env, type, expr);
    }

    public RexLiteral makeLiteral(boolean b) {
        return makeLiteral(b ? Boolean.TRUE : Boolean.FALSE,
                typeFactory.createJavaType(boolean.class));
    }

    public RexLiteral makeLiteral(long i) {
        return makeLiteral(BigInteger.valueOf(i),
                typeFactory.createJavaType(long.class));
    }

    public RexLiteral makeLiteral(double d) {
        return makeLiteral(new Double(d), typeFactory.createJavaType(double.class));
    }

    public RexLiteral makeLiteral(String s) {
        return makeLiteral(s, typeFactory.createJavaType(String.class));
    }

    public RexNode makeCase(RexNode rexCond, RexNode rexTrueCase, RexNode rexFalseCase) {
        throw Util.needToImplement(this);
    }

    public SqlOperator getOperator(RexKind kind) {
        throw Util.needToImplement(this);
    }

    public RexNode makeCast(SaffronType type, RexNode exp) {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression java = (JavaRowExpression) exp;
            final OJClass ojClass = OJUtil.typeToOJClass(type);
            final CastExpression castExpr = new CastExpression(
                    ojClass, java.expression);
            return new JavaRowExpression(java.env, type, castExpr);
        }
        return super.makeCast(type, exp);
    }

    private static class OJTranslator {
        public ExpressionList toJava(List args) {
            return null;
        }

        public Expression toJava(RexNode exp) {
            throw Util.needToImplement(this);
        }
    }
}

// End JavaRexBuilder.java
