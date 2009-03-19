/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package org.eigenbase.oj.util;

import java.util.List;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * Extends {@link RexBuilder} to builds row-expressions including those
 * involving Java code.
 *
 * @author jhyde
 * @version $Id$
 * @see JavaRowExpression
 * @since Nov 23, 2003
 */
public class JavaRexBuilder
    extends RexBuilder
{
    //~ Instance fields --------------------------------------------------------

    OJTranslator translator = new OJTranslator();

    //~ Constructors -----------------------------------------------------------

    public JavaRexBuilder(RelDataTypeFactory typeFactory)
    {
        super(typeFactory);
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode makeFieldAccess(
        RexNode exp,
        String fieldName)
    {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression jexp = (JavaRowExpression) exp;
            final FieldAccess fieldAccess =
                new FieldAccess(
                    jexp.getExpression(),
                    fieldName);
            return makeJava(jexp.env, fieldAccess);
        } else {
            return super.makeFieldAccess(exp, fieldName);
        }
    }

    /**
     * Creates a call to a Java method.
     *
     * @param exp Target of the method
     * @param methodName Name of the method
     * @param args Argument expressions; null means no arguments
     *
     * @return Method call
     */
    public RexNode createMethodCall(
        Environment env,
        RexNode exp,
        String methodName,
        List<RexNode> args)
    {
        ExpressionList ojArgs = translator.toJava(args);
        Expression ojExp = translator.toJava(exp);
        return makeJava(
            env,
            new MethodCall(ojExp, methodName, ojArgs));
    }

    public RexNode makeJava(
        Environment env,
        Expression expr)
    {
        final OJClass ojClass;
        try {
            ojClass = expr.getType(env);
        } catch (Exception e) {
            throw Util.newInternal(
                e,
                "Error deriving type of expression " + expr);
        }
        RelDataType type = OJUtil.ojToType(this.typeFactory, ojClass);
        return new JavaRowExpression(env, type, expr);
    }

    public RexNode makeCase(
        RexNode rexCond,
        RexNode rexTrueCase,
        RexNode rexFalseCase)
    {
        throw Util.needToImplement(this);
    }

    public RexNode makeCast(
        RelDataType type,
        RexNode exp)
    {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression java = (JavaRowExpression) exp;
            final OJClass ojClass = OJUtil.typeToOJClass(type, typeFactory);
            final CastExpression castExpr =
                new CastExpression(
                    ojClass,
                    java.getExpression());
            return new JavaRowExpression(java.env, type, castExpr);
        }
        return super.makeCast(type, exp);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class OJTranslator
    {
        public ExpressionList toJava(List<RexNode> args)
        {
            return null;
        }

        public Expression toJava(RexNode exp)
        {
            throw Util.needToImplement(this);
        }
    }
}

// End JavaRexBuilder.java
