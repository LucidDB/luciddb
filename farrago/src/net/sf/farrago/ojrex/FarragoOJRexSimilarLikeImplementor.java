/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2005-2007 Xiaoyang Luo
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

import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FarragoOJRexSimilarLikeImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for builtin functions <code>
 * SIMILAR</code> and <code>LIKE</code>.
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class FarragoOJRexSimilarLikeImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final boolean similar;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexSimilarLikeImplementor(
        boolean similar)
    {
        this.similar = similar;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        Variable varResult = null;
        Expression nullTest = null;
        OJClass ojPatternClass = null;
        OJClass ojMatcherClass = null;
        Variable varPattern;
        Variable varMatcher;
        boolean hasEscape = false;
        boolean atRuntime = true;
        Expression expForPattern = null;
        ExpressionList emptyArguments = new ExpressionList();

        RelDataType retType = call.getType();

        if (SqlTypeUtil.isJavaPrimitive(retType) && !retType.isNullable()) {
            OJClass retClass =
                OJUtil.typeToOJClass(
                    retType,
                    translator.getFarragoTypeFactory());
            varResult = translator.getRelImplementor().newVariable();
            translator.addStatement(
                new VariableDeclaration(
                    TypeName.forOJClass(retClass),
                    new VariableDeclarator(
                        varResult.toString(),
                        null)));
        } else {
            varResult = translator.createScratchVariable(retType);
        }

        try {
            Class classPattern = Class.forName("java.util.regex.Pattern");
            Class classMatcher = Class.forName("java.util.regex.Matcher");
            ojPatternClass = OJClass.forClass(classPattern);
            ojMatcherClass = OJClass.forClass(classMatcher);
        } catch (Exception e) {
            assert (false);
        }

        if (operands.length == 3) {
            hasEscape = true;
        }
        if ((call.operands[1] instanceof RexLiteral)
            && (!hasEscape
                || (hasEscape && (call.operands[2] instanceof RexLiteral))))
        {
            atRuntime = false;
        }

        if (atRuntime) {
            expForPattern = Literal.constantNull();
        } else {
            String escapeStr = null;
            RexLiteral literal;
            NlsString str;
            literal = (RexLiteral) call.operands[1];
            str = (NlsString) literal.getValue();
            String sqlPattern = str.getValue();
            if (hasEscape) {
                literal = (RexLiteral) call.operands[2];
                str = (NlsString) literal.getValue();
                escapeStr = str.getValue();
            }
            String javaPattern = null;
            if (similar) {
                javaPattern =
                    RuntimeTypeUtil.SqlToRegexSimilar(
                        sqlPattern,
                        escapeStr);
            } else {
                javaPattern =
                    RuntimeTypeUtil.SqlToRegexLike(
                        sqlPattern,
                        escapeStr);
            }
            expForPattern =
                new MethodCall(
                    ojPatternClass,
                    "compile",
                    new ExpressionList(
                        Literal.makeLiteral(javaPattern)));
        }

        varPattern =
            translator.createScratchVariableWithExpression(
                ojPatternClass,
                expForPattern);

        varMatcher =
            translator.createScratchVariableWithExpression(
                ojMatcherClass,
                Literal.constantNull());

        for (int i = 0; i < operands.length; i++) {
            nullTest =
                translator.createNullTest(
                    call.operands[i],
                    operands[i],
                    nullTest);
        }

        StatementList stmtList = new StatementList();
        if (atRuntime) {
            Variable varJavaPatternStr;
            OJClass ojStringClass = null;
            OJClass ojUtilClass = null;
            try {
                Class classString = Class.forName("java.lang.String");
                Class classUtil =
                    Class.forName(
                        "net.sf.farrago.type.runtime.RuntimeTypeUtil");

                ojStringClass = OJClass.forClass(classString);
                ojUtilClass = OJClass.forClass(classUtil);
            } catch (Exception e) {
                assert (false);
            }
            varJavaPatternStr = translator.getRelImplementor().newVariable();
            translator.addStatement(
                new VariableDeclaration(
                    TypeName.forOJClass(ojStringClass),
                    new VariableDeclarator(
                        varJavaPatternStr.toString(),
                        null)));
            ExpressionList likeArguments =
                new ExpressionList(
                    new MethodCall(
                        operands[1],
                        "toString",
                        emptyArguments));

            if (hasEscape) {
                likeArguments.add(operands[2]);
            } else {
                likeArguments.add(Literal.constantNull());
            }
            String funcName = null;
            if (similar) {
                funcName = "SqlToRegexSimilar";
            } else {
                funcName = "SqlToRegexLike";
            }

            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varJavaPatternStr,
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            ojUtilClass,
                            funcName,
                            likeArguments))));

            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varPattern,
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            ojPatternClass,
                            "compile",
                            new ExpressionList(
                                varJavaPatternStr)))));

            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varMatcher,
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            varPattern,
                            "matcher",
                            new ExpressionList(
                                operands[0])))));
        } else {
            IfStatement ifStmt =
                new IfStatement(
                    new BinaryExpression(
                        varMatcher,
                        BinaryExpression.EQUAL,
                        Literal.constantNull()),
                    new StatementList(
                        new ExpressionStatement(
                            new AssignmentExpression(
                                varMatcher,
                                AssignmentExpression.EQUALS,
                                new MethodCall(
                                    varPattern,
                                    "matcher",
                                    new ExpressionList(
                                        operands[0]))))),
                    new StatementList(
                        new ExpressionStatement(
                            new MethodCall(
                                varMatcher,
                                "reset",
                                new ExpressionList(
                                    operands[0])))));
            stmtList.add(ifStmt);
        }

        Expression matcherResult =
            new MethodCall(
                varMatcher,
                "matches",
                emptyArguments);

        translator.addAssignmentStatement(
            stmtList,
            matcherResult,
            call.getType(),
            varResult,
            false);

        // All the builtin function returns null if
        // one of the arguements is null.

        if (nullTest != null) {
            translator.addStatement(
                new IfStatement(
                    nullTest,
                    new StatementList(
                        translator.createSetNullStatement(varResult, true)),
                    stmtList));
        } else {
            for (int i = 0; i < stmtList.size(); i++) {
                translator.addStatement(stmtList.get(i));
            }
        }
        return varResult;
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexSimilarLikeImplementor.java
