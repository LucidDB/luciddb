/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
    public final String name;

    /**
     * See {@link SqlKind}. It's possible to have a name that doesn't match
     * the kind
     */
    public final SqlKind kind;

    /**
     * The precedence with which this operator binds to the expression to the
     * left. This is less than the right precedence if the operator is
     * left-associative.
     */
    public final int leftPrec;

    /**
     * The precedence with which this operator binds to the expression to the
     * right. This is more than the left precedence if the operator is
     * left-associative.
     */
    public final int rightPrec;

    /** used to get the return type of operator/function */
    private final ReturnTypeInference returnTypeInference;

    /** used to inference unknown params */
    private final UnknownParamInference unknownParamTypeInference;

    /** used to validate operands */
    protected final OperandsTypeChecking operandsCheckingRule;

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
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking operandsCheckingRule)
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
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking operandsCheckingRule)
    {
        this(name, kind, (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0), typeInference,
            paramTypeInference, operandsCheckingRule);
    }

    //~ Methods ---------------------------------------------------------------

    public OperandsTypeChecking getOperandsCheckingRule()
    {
        return this.operandsCheckingRule;
    }

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        if (null != operandsCheckingRule) {
            return new OperandsCountDescriptor(operandsCheckingRule.getArgCount());
        }

        // If you see this error you need to overide this method
        // or give operandsCheckingRule a value.
        throw Util.needToImplement(this);
    }

    public String toString()
    {
        return name;
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
    public SqlCall rewriteCall(
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
     *    operator is an aggregate function.
     */
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlValidator.Scope operandScope)
    {
        assert call.operator == this;
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
     * {@link #getType(org.eigenbase.sql.SqlValidator, org.eigenbase.sql.SqlValidator.Scope, org.eigenbase.sql.SqlCall)}
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
     * ways. If they have a {@link org.eigenbase.sql.type.ReturnTypeInference},
     * it is used (prefered since it enables code reuse); otherwise, operators with unusual type inference schemes
     * should override
     * {@link #getType(org.eigenbase.sql.SqlValidator, org.eigenbase.sql.SqlValidator.Scope, org.eigenbase.reltype.RelDataTypeFactory, org.eigenbase.sql.type.CallOperands)}
     */
    public final RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        // Check that there's the right number of arguments.
        checkNumberOfArg(validator, operandsCheckingRule, call);

        checkArgTypes(call, validator, scope, true);

        // Now infer the result type.
        CallOperands.SqlCallOperands callOperands =
            new CallOperands.SqlCallOperands(validator,  scope, call);
        RelDataType ret =
            getType(validator, scope, validator.typeFactory, callOperands);
        validator.setValidatedNodeType(call, ret);
        return ret;
    }

    /**
     * Figure out the type of the return of this function.
     * We have already checked that the number and types of arguments are as
     * required.
     * If no {@link ReturnTypeInference} object was set, you must override this
     * method.
     * This method can be called by the {@link SqlValidator validator}
     * in which case validator!=null and scope!=null<br>
     * or in the {@link org.eigenbase.sql2rel.SqlToRelConverter} in which
     * case validator==null and scope==null
     */
    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
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

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope, boolean throwOnFailure)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        if (null == operandsCheckingRule) {
            // If you see this you must either give operandsCheckingRule a value
            // or override this method.
            throw Util.needToImplement(this);
        }

        return operandsCheckingRule.check(validator, scope, call, throwOnFailure);
    }

    protected void checkNumberOfArg(
        SqlValidator validator, OperandsTypeChecking argType,
        SqlCall call)
    {
        OperandsCountDescriptor od =
            call.operator.getOperandsCountDescriptor();
        if (!od.isVariadic()
                && !od.getPossibleNumOfOperands().contains(
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
        assert (null != operandsCheckingRule) : "If you see this, assign operandsCheckingRule a value "
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

    public UnknownParamInference getUnknownParamTypeInference()
    {
        return unknownParamTypeInference;
    }

    public boolean isAggregator()
    {
        return "SUM".equals(name) || "COUNT".equals(name);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A class that describes how many operands a operator can take
     */
    public static class OperandsCountDescriptor
    {
        public static final OperandsCountDescriptor variadicCountDescriptor =
                    new OperandsCountDescriptor();
        public static final OperandsCountDescriptor niladicCountDescriptor =
                    new OperandsCountDescriptor(0);
        public static final OperandsCountDescriptor One =
                    new OperandsCountDescriptor(1);
        public static final OperandsCountDescriptor Two =
                    new OperandsCountDescriptor(2);
        public static final OperandsCountDescriptor Three =
                    new OperandsCountDescriptor(3);
        public static final OperandsCountDescriptor Four =
                    new OperandsCountDescriptor(4);

        List possibleList;
        boolean isVariadic;

        /**
         * This constructor should only be called internally from this class
         * and only when creating a variadic count descriptor
         */
        private OperandsCountDescriptor()
        {
            possibleList = null;
            isVariadic = true;
        }

        private OperandsCountDescriptor(Integer[] possibleCounts)
        {
            possibleList = Collections.unmodifiableList(Arrays.asList(
                possibleCounts));
            isVariadic = false;
        }

        public OperandsCountDescriptor(int count)
        {
            this(new Integer[]{ new Integer(count) });
            isVariadic = false;
        }

        public OperandsCountDescriptor(
            int count1,
            int count2)
        {
            this(new Integer[]{ new Integer(count1), new Integer(count2) });
        }

        public OperandsCountDescriptor(
            int count1,
            int count2,
            int count3)
        {
            this(new Integer[]{ new Integer(count1),
                                new Integer(count2),
                                new Integer(count3) });
        }

        /**
         * Returns a unmodifiable list with items containing how many operands
         * a operator can accept
         * @pre isVariadic == false
         */
        public List getPossibleNumOfOperands()
        {
            Util.pre(!isVariadic, "!isVariadic");
            return possibleList;
        }

        public boolean isVariadic()
        {
            return isVariadic;
        }
    }

}


// End SqlOperator.java
