/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A <code>SqlOperator</code> is a type of node in a SQL parse tree (it is NOT
 * a node in a SQL parse tree). It includes functions, operators such as '=',
 * and syntactic constructs such as 'case' statements.
 */
public abstract class SqlOperator
{
    //~ Static fields/initializers --------------------------------------------

    public static final String NL = System.getProperty("line.separator");

    /**
     * A call signature gets built with this string. It can then be
     * replaced with a operator or a function name. This still works in the case
     * the operator or function name has the same character sequence(s) as this
     * string.
     * In the case a user defines a type with a name with a match to this string
     * there could be a potential problem. To avoid this, all types are outputted
     * WITH CAPITAL letters, and this string must therefore consist
     * of at least one lowercase letter.
     *
     *<p>
     *
     * REVIEW jvs 2-Dec-2004:  what about user-defined types with quoted
     * names?  They could be all-lowercase.  Do we really have to use such
     * a hokey mechanism?
     */
    private static final String ANONYMOUS_REPLACE = "xyzzy";

    //~ Instance fields -------------------------------------------------------

    /**
     * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
     */
    private final String name;

    /**
     * See {@link SqlKind}. It's possible to have a name that doesn't match
     * the kind
     */
    private final SqlKind kind;

    /**
     * The precedence with which this operator binds to the expression to the
     * left. This is less than the right precedence if the operator is
     * left-associative.
     */
    private final int leftPrec;

    /**
     * The precedence with which this operator binds to the expression to the
     * right. This is more than the left precedence if the operator is
     * left-associative.
     */
    private final int rightPrec;

    /** used to infer the return type of a call to this operator */
    private final SqlReturnTypeInference returnTypeInference;

    /** used to infer types of unknown operands */
    private final SqlOperandTypeInference unknownParamTypeInference;

    /** used to validate operand types */
    protected final SqlOperandTypeChecker operandsCheckingRule;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an operator.
     *
     * @pre kind != null
     */
    // @pre paramTypes != null
    protected SqlOperator(
        String name,
        SqlKind kind,
        int leftPrecedence,
        int rightPrecedence,
        SqlReturnTypeInference typeInference,
        SqlOperandTypeInference paramTypeInference,
        SqlOperandTypeChecker operandsCheckingRule)
    {
        Util.pre(kind != null, "kind != null");
        this.name = name;
        this.kind = kind;
        this.leftPrec = leftPrecedence;
        this.rightPrec = rightPrecedence;
        this.returnTypeInference = typeInference;
        this.unknownParamTypeInference = paramTypeInference;
        this.operandsCheckingRule = operandsCheckingRule;
    }

    /**
     * Creates an operator specifying left/right associativity.
     */
    protected SqlOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean isLeftAssoc,
        SqlReturnTypeInference typeInference,
        SqlOperandTypeInference paramTypeInference,
        SqlOperandTypeChecker operandsCheckingRule)
    {
        this(name, kind, (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0), typeInference,
            paramTypeInference, operandsCheckingRule);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlOperandTypeChecker getOperandsCheckingRule()
    {
        return this.operandsCheckingRule;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        if (null != operandsCheckingRule) {
            return operandsCheckingRule.getOperandCountRange();
        }

        // If you see this error you need to overide this method
        // or give operandsCheckingRule a value.
        throw Util.needToImplement(this);
    }

    public String getName()
    {
        return name;
    }

    public SqlKind getKind()
    {
        return kind;
    }

    public String toString()
    {
        return name;
    }

    public int getLeftPrec()
    {
        return leftPrec;
    }

    public int getRightPrec()
    {
        return rightPrec;
    }

    /**
     * Returns a template describing how the operator signature is to be built.
     * E.g for the binary + operator the template looks like "{1} {0} {2}"
     * {0} is the operator, subsequent numbers are operands.
     * If null is returned, the default template will be used which
     * is opname(operand0, operand1, ...)
     *
     * @param operandsCount is used with functions that can take a variable
     * number of operands.
     */
    protected String getSignatureTemplate(final int operandsCount)
    {
        return null;
    }

    /**
     * Returns the syntactic type of this operator.
     *
     * @post return != null
     */
    public abstract SqlSyntax getSyntax();

    /**
     * Runs a series of tests to verify that this operator validates and
     * executes correctly.
     *
     * <p>The specific implementation should call the various
     * <code>checkXxx</code> methods in the {@link SqlTester} interface. The
     * test harness may call the test method several times with different
     * implementations of {@link SqlTester} -- perhaps one which uses the
     * farrago calculator, and another which implements operators by generating
     * Java code.
     *
     * <p>The default implementation does nothing.
     *
     * <p>An example test function for the sin operator:
     *
     * <blockqoute><pre><code>void test(SqlTester tester) {
     *     tester.checkScalar("sin(0)", "0");
     *     tester.checkScalar("sin(1.5707)", "1");
     * }</code></pre></blockqoute>
     *
     * @param tester The tester to use.
     */
    public void test(SqlTester tester)
    {
    }

    /**
     * Creates a call to this operand with an array of operands.
     */
    public SqlCall createCall(
        SqlNode [] operands,
        SqlParserPos pos)
    {
        return new SqlCall(this, operands, pos);
    }

    /**
     * Creates a call to this operand with no operands.
     */
    public SqlCall createCall(SqlParserPos pos)
    {
        return createCall(SqlNode.emptyArray, pos);
    }

    /**
     * Creates a call to this operand with a single operand.
     */
    public SqlCall createCall(
        SqlNode operand,
        SqlParserPos pos)
    {
        return createCall(
            new SqlNode [] { operand },
            pos);
    }

    /**
     * Creates a call to this operand with two operands.
     */
    public SqlCall createCall(
        SqlNode operand1,
        SqlNode operand2,
        SqlParserPos pos)
    {
        return createCall(
            new SqlNode [] { operand1, operand2 },
            pos);
    }

    /**
     * Creates a call to this operand with three operands.
     */
    public SqlCall createCall(
        SqlNode operand1,
        SqlNode operand2,
        SqlNode operand3,
        SqlParserPos pos)
    {
        return createCall(
            new SqlNode [] { operand1, operand2, operand3 },
            pos);
    }

    /**
     * Rewrites a call to this operator.  Some operators are implemented as
     * trivial rewrites (e.g. NULLIF becomes CASE).  However, we don't do this
     * at createCall time because we want to preserve the original SQL syntax
     * as much as possible; instead, we do this before the call is validated
     * (so the trivial operator doesn't need its own implementation of type
     * derivation methods).  The default implementation is to just return the
     * original call without any rewrite.
     *
     * @param call to be rewritten
     *
     * @return rewritten call
     */
    public SqlNode rewriteCall(
        SqlCall call)
    {
        return call;
    }

    /**
     * Writes a SQL representation of a call to this operator to a writer,
     * including parentheses if the operators on either side are of greater
     * precedence.
     *
     * <p>The default implementation of this method delegates to
     * {@link SqlSyntax#unparse}.
     */
    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        getSyntax().unparse(writer, this, operands, leftPrec, rightPrec);
    }

    // override Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof SqlOperator)) {
            return false;
        }
        SqlOperator other = (SqlOperator) obj;
        return name.equals(other.name) && kind.equals(other.kind);
    }

    // override Object
    public int hashCode()
    {
        return (kind.getOrdinal() * 31) + name.hashCode();
    }

    /**
     * Validates a call to this operator.
     *
     * <p>A typical implementation of this method first validates the
     * operands, then performs some operator-specific logic.
     * The default implementation just validates the operands.
     *
     * <p>This method is the default implementation of
     * {@link SqlCall#validate}; but note that some sub-classes of
     * {@link SqlCall} never call this method.
     *
     * @param call the call to this operator
     * @param validator the active validator
     * @param scope validator scope
     * @param operandScope validator scope in which to validate operands to
     *    this call. Usually equal to scope, but may be different if the
     */
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        assert call.getOperator() == this;
        final SqlNode[] operands = call.getOperands();
        for (int i = 0; i < operands.length; i++) {
            operands[i].validateExpr(validator, operandScope);
        }
    }

    /**
     * Deduces the type of a call to this operator, using information from the
     * operands.
     *
     * Normally (as in the default implementation), only the types of the operands
     * are needed  to determine the type of the call.  If the call type depends
     * on the values of the operands, then override
     * {@link #getType(SqlValidator, SqlValidatorScope, RelDataTypeFactory, CallOperands)}
     */
    public final RelDataType getType(
        RelDataTypeFactory typeFactory,
        RexNode [] exprs)
    {
        return getType(null, null, typeFactory,
            new CallOperands.RexCallOperands(typeFactory,  this, exprs));
    }

    /**
     * Deduces the type of a call to this operator. To do this, the method
     * first needs to recursively deduce the types of its arguments, using
     * the validator and scope provided.
     *
     * <p>Particular operators can affect the behavior of this method in two
     * ways. If they have a {@link SqlReturnTypeInference},
     * it is used (prefered since it enables code reuse);
     * otherwise, operators with unusual type inference schemes
     * should override
     * {@link #getType(SqlValidator, SqlValidatorScope, RelDataTypeFactory, CallOperands)}
     */
    public final RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        // Let subclasses know what's up.
        preValidateCall(validator, scope, call);
        
        // Check that there's the right number of arguments.
        checkArgCount(validator, operandsCheckingRule, call);

        checkArgTypes(call, validator, scope, true);

        // Now infer the result type.
        CallOperands.SqlCallOperands callOperands =
            new CallOperands.SqlCallOperands(validator,  scope, call);
        RelDataType ret = getType(
            validator, scope, validator.getTypeFactory(), callOperands);
        validator.setValidatedNodeType(call, ret);
        return ret;
    }

    /**
     * Receives notification that validation of a call to this operator
     * is beginning.  Subclasses can supply custom behavior;
     * default implementation does nothing.
     *
     * @param validator invoking validator
     *
     * @param scope validation scope
     *
     * @param call the call being validated
     */
    protected void preValidateCall(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
    }

    /**
     * Deduces the type of the return of a call to this operator.
     * We have already checked that the number and types of arguments are as
     * required.
     * If no {@link SqlReturnTypeInference} object was set, you must override this
     * method.
     *
     * <p>This method can be called by the {@link SqlValidator validator}
     * in which case validator != null and scope != null;
     * or by the {@link org.eigenbase.sql2rel.SqlToRelConverter} in which
     * case validator == null and scope == null
     *
     * XXX converter should provide mock validator
     */
    protected RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        if (returnTypeInference != null) {
            return returnTypeInference.getType(
                validator, scope, typeFactory, callOperands);
        }

        // Derived type should have overridden this method, since it didn't
        // supply a type inference rule.
        throw Util.needToImplement(this);
    }

    // TODO jvs 2-June-2005: Change all operators to always supply a
    // SqlOperandTypeChecker and eliminate this inheritance-based override
    // mechanism.
    /**
     * Makes sure that the number and types of arguments are allowable.
     */
    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope, boolean throwOnFailure)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        if (null == operandsCheckingRule) {
            // If you see this you must either give operandsCheckingRule a value
            // or override this method.
            throw Util.needToImplement(this);
        }

        return operandsCheckingRule.checkCall(
            validator, scope, call, throwOnFailure);
    }

    private void checkArgCount(
        SqlValidator validator,
        SqlOperandTypeChecker argType,
        SqlCall call)
    {
        SqlOperandCountRange od =
            call.getOperator().getOperandCountRange();
        if (!od.isVariadic()
                && !od.getAllowedList().contains(
                    new Integer(call.operands.length))) {
            throw validator.newValidationError(call,
                EigenbaseResource.instance().newWrongNumOfArguments());
        }
    }

    /**
     * Returns a string describing the expected argument types of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    public String getAllowedSignatures()
    {
        return getAllowedSignatures(name);
    }

    /**
     * Returns a string describing the expected argument types of a call, e.g.
     * "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name SUBSTRING can
     * be replaced by a specifed name.
     */
    public String getAllowedSignatures(String opNameToUse)
    {
        assert (null != operandsCheckingRule)
            : "If you see this, assign operandsCheckingRule a value "
            + "or override this function";
        return replaceAnonymous(
            operandsCheckingRule.getAllowedSignatures(this),
            opNameToUse).trim();
    }

    /**
     * Returns the same as {@link #getAnonymousSignature} with the exception that
     * {@link this.name} is the function/operator signature string
     * @param list
     * @return
     */
    public String getSignature(final ArrayList list)
    {
        return replaceAnonymous(
            getAnonymousSignature(list),
            name);
    }

    /**
     * Returns a string of all allowed types, permutated with an anonymous
     * string represented by {@link #ANONYMOUS_REPLACE}.
     */
    public String getAnonymousSignature(final ArrayList typeList)
    {
        StringBuffer ret = new StringBuffer();
        String template = getSignatureTemplate(typeList.size());
        if (null == template) {
            ret.append("'");
            ret.append(ANONYMOUS_REPLACE);
            ret.append("(");
            for (int i = 0; i < typeList.size(); i++) {
                if (i > 0) {
                    ret.append(", ");
                }
                ret.append("<" + typeList.get(i).toString().toUpperCase() + ">");
            }
            ret.append(")'");
        } else {
            Object [] values = new Object[typeList.size() + 1];
            values[0] = ANONYMOUS_REPLACE;
            ret.append("'");
            for (int i = 0; i < typeList.size(); i++) {
                values[i + 1] = "<" +
                    typeList.get(i).toString().toUpperCase() + ">";
            }
            ret.append(MessageFormat.format(template, values));
            ret.append("'");
            assert (typeList.size() + 1) == values.length;
        }

        return ret.toString();
    }

    protected String replaceAnonymous(
        String original,
        String name)
    {
        return original.replaceAll(ANONYMOUS_REPLACE, name);
    }

    public SqlOperandTypeInference getUnknownParamTypeInference()
    {
        return unknownParamTypeInference;
    }

    /**
     * Returns whether this operator is an aggregate function.
     * By default, subclass type is used (an instance of SqlAggFunction
     * is assumed to be an aggregator; anything else is not).
     *
     * @return whether this operator is an aggregator
     */
    public boolean isAggregator()
    {
        return (this instanceof SqlAggFunction);
    }

    /**
     * Accepts a {@link SqlVisitor}, and tells it to visit each child.
     *
     * @param visitor Visitor.
     */
    public void acceptCall(SqlVisitor visitor, SqlCall call)
    {
        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            if (operand == null) {
                continue;
            }
            visitor.visitChild(call, i, operand);
        }
    }

    /**
     * Method to check if call to this function is monotonic.  Default
     * implementation is to return false.
     */
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        return false; 
    }
}


// End SqlOperator.java
