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
package org.eigenbase.oj.util;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexKind;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.math.BigDecimal;
import java.util.List;

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

    public JavaRexBuilder(RelDataTypeFactory typeFactory) {
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
        RelDataType type = OJUtil.ojToType(this._typeFactory, ojClass);
        return new JavaRowExpression(env, type, expr);
    }


    public RexNode makeCase(RexNode rexCond, RexNode rexTrueCase,
                            RexNode rexFalseCase) {
        throw Util.needToImplement(this);
    }

    public RexNode makeCast(RelDataType type, RexNode exp) {
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
