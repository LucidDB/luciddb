/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;


/**
 * An operator describing a <code>CASE</code>, <code>NULLIF</code> or
 * <code>COALESCE</code> expression. All of these forms are normalized at parse
 * time to a to a simple <code>CASE</code> statement like this:
 *
 * <blockquote><code><pre>CASE
 *   WHEN &lt;when expression_0&gt; THEN &lt;then expression_0&gt;
 *   WHEN &lt;when expression_1&gt; THEN &lt;then expression_1&gt;
 *   ...
 *   WHEN &lt;when expression_N&gt; THEN &ltthen expression_N&gt;
 *   ELSE &lt;else expression&gt;
 * END</pre></code></blockquote>
 *
 * The switched form of the <code>CASE</code> statement is normalized to the
 * simple form by inserting calls to the <code>=</code> operator. For example,
 *
 * <blockquote<code><pre>CASE x + y
 *   WHEN 1 THEN 'fee'
 *   WHEN 2 THEN 'fie'
 *   ELSE 'foe'
 * END</pre></code></blockquote>
 *
 * becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END</pre></code></blockquote>
 *
 * <p>REVIEW jhyde 2004/3/19 Does <code>Equals</code> handle NULL semantics
 * correctly?</p>
 *
 * <p><code>COALESCE(x, y, z)</code> becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END</pre></code></blockquote></p>
 *
 * <p><code>NULLIF(x, -1)</code> becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END</pre></code></blockquote></p>
 *
 * <p>Note that some of these normalizations cause expressions to be
 * duplicated. This may make it more difficult to write optimizer rules
 * (because the rules will have to deduce that expressions are equivalent).
 * It also requires that some part of the planning process (probably the
 * generator of the calculator program) does common sub-expression
 * elimination.</p>
 *
 * <p>REVIEW jhyde 2004/3/19. Expanding expressions at parse time has some
 * other drawbacks. It is more difficult to give meaningful validation errors:
 * given <code>COALESCE(DATE '2004-03-18', 3.5)</code>, do we issue a
 * type-checking error against a <code>CASE</code> operator? Second, I'd like
 * to use the {@link SqlNode} object model to generate SQL to send to 3rd-party
 * databases, but there's now no way to represent a call to COALESCE or NULLIF.
 * All in all, it would be better to have operators for COALESCE, NULLIF, and
 * both simple and switched forms of CASE, then translate to simple CASE when
 * building the {@link org.eigenbase.rex.RexNode} tree.</p>

 * <p>The arguments are physically represented as follows:<ul>
 * <li>The <i>when</i> expressions are stored in a {@link SqlNodeList}
 *     whenList.</li>
 * <li>The <i>then</i> expressions are stored in a {@link SqlNodeList}
 *     thenList.</li>
 * <li>The <i>else</i> expression is stored as a regular {@link SqlNode}.</li>
 * </ul></p>
 *
 * @author Wael Chatila
 * @since Mar 14, 2004
 * @version $Id$
 **/
public class SqlCaseOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlCaseOperator()
    {
        super("CASE", SqlKind.Case, 1, true, null,
            UnknownParamInference.useReturnType, null);
    }

    //~ Methods ---------------------------------------------------------------

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlValidator.Scope operandScope)
    {
        final SqlCase sqlCase = (SqlCase) call;
        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        final SqlNode elseOperand = sqlCase.getElseOperand();
        for (int i = 0; i < whenOperands.size(); i++) {
            SqlNode operand = whenOperands.get(i);
            operand.validateExpr(validator, operandScope);
        }
        for (int i = 0; i < thenOperands.size(); i++) {
            SqlNode operand = thenOperands.get(i);
            operand.validateExpr(validator, operandScope);
        }
        if (elseOperand != null) {
            elseOperand.validateExpr(validator, operandScope);
        }
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        boolean throwOnFailure)
    {
        SqlCase caseCall = (SqlCase) call;
        SqlNodeList whenList = caseCall.getWhenOperands();
        SqlNodeList thenList = caseCall.getThenOperands();
        assert (whenList.size() == thenList.size());

        //checking that search conditions are ok...
        for (int i = 0; i < whenList.size(); i++) {
            SqlNode node = whenList.get(i);

            //should throw validation error if something wrong...
            RelDataType type = validator.deriveType(scope, node);
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                if (throwOnFailure) {
                    throw validator.newValidationError(node,
                        EigenbaseResource.instance().newExpectedBoolean());
                }
                return false;
            }
        }

        boolean foundNotNull = false;
        for (int i = 0; i < thenList.size(); i++) {
            SqlNode node = thenList.get(i);
            if (!SqlUtil.isNullLiteral(node, false)) {
                foundNotNull = true;
            }
        }

        if (!SqlUtil.isNullLiteral(caseCall.getElseOperand(), false)) {
            foundNotNull = true;
        }

        if (!foundNotNull) {
            // according to the sql standard we can not have all of the THEN
            // statements and the ELSE returning null
            if (throwOnFailure) {
                throw EigenbaseResource.instance().newMustNotNullInElse();
            }
            return false;
        }
        return true;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        if (null==validator) {
            return getTypeFromRexs(typeFactory, callOperands.collectTypes());
        }
        return getTypeFromValidator(callOperands, validator, scope, typeFactory);
    }

    private RelDataType getTypeFromValidator(CallOperands callOperands,
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory)
    {
        SqlCase caseCall = (SqlCase) callOperands.getUnderlyingObject();
        SqlNodeList thenList = caseCall.getThenOperands();
        ArrayList nullList = new ArrayList();
        RelDataType [] argTypes = new RelDataType[thenList.size() + 1];
        for (int i = 0; i < thenList.size(); i++) {
            SqlNode node = thenList.get(i);
            argTypes[i] = validator.deriveType(scope, node);
            if (SqlUtil.isNullLiteral(node, false)) {
                nullList.add(node);
            }
        }
        SqlNode elseOp = caseCall.getElseOperand();
        argTypes[argTypes.length - 1] =
            validator.deriveType(
                scope,
                caseCall.getElseOperand());
        if (SqlUtil.isNullLiteral(elseOp, false)) {
            nullList.add(elseOp);
        }

        RelDataType ret = SqlTypeUtil.getNullableBiggest(typeFactory, argTypes);
        if (null == ret) {
            throw validator.newValidationError(caseCall,
                    EigenbaseResource.instance().newIllegalMixingOfTypes());
        }
        for (int i = 0; i < nullList.size(); i++) {
            SqlNode node = (SqlNode) nullList.get(i);
            validator.setValidatedNodeType(node, ret);
        }
        return ret;
    }

    private RelDataType getTypeFromRexs(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes)
    {
        assert (argTypes.length % 2) == 1 : "odd number of arguments expected: "
        + argTypes.length;
        assert argTypes.length > 1 : argTypes.length;
        RelDataType [] thenTypes =
            new RelDataType[((argTypes.length - 1) / 2) + 1];
        for (int i = 0, j = 1; j < (argTypes.length - 1); i++, j += 2) {
            thenTypes[i] = argTypes[j];
        }

        thenTypes[thenTypes.length - 1] = argTypes[argTypes.length - 1];
        return SqlTypeUtil.getNullableBiggest(typeFactory, thenTypes);
    }


    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlNode [] operands,
        SqlParserPos pos)
    {
        return new SqlCase(this, operands, pos);
    }

    public SqlCase createCall(
        SqlNode caseIdentifier,
        SqlNodeList whenList,
        SqlNodeList thenList,
        SqlNode elseClause,
        SqlParserPos pos)
    {
        SqlStdOperatorTable stdOps = SqlStdOperatorTable.instance();
        if (null != caseIdentifier) {
            List list = whenList.getList();
            for (int i = 0; i < list.size(); i++) {
                SqlNode e = (SqlNode) list.get(i);
                list.set(
                    i,
                    stdOps.equalsOperator.createCall(caseIdentifier, e,
                        pos));
            }
        }

        if (null == elseClause) {
            elseClause = SqlLiteral.createNull(pos);
        }

        return (SqlCase) createCall(
            new SqlNode [] { whenList, thenList, elseClause },
            pos);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        throw Util.needToImplement("need to implement");
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testCase(tester);
    }
}


// End SqlCaseOperator.java
