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
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.TypeUtil;
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
     */
    private static final String ANONYMOUS_REPLACE = "anystringwilldothatisntthesameasatype";

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
    private final ReturnTypeInference typeInference;

    /** used to inference unknown params */
    private final UnknownParamInference paramTypeInference;

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

//        Util.pre(operandsCheckingRule != null, "operandsCheckingRule != null");
        this.name = name;
        this.kind = kind;
        this.leftPrec = leftPrecedence;
        this.rightPrec = rightPrecedence;
        this.typeInference = typeInference;
        this.paramTypeInference = paramTypeInference;
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
        ParserPosition pos)
    {
        return new SqlCall(this, operands, pos);
    }

    /**
     * Creates a call to this operand with no operands.
     */
    public SqlCall createCall(ParserPosition pos)
    {
        return createCall(SqlNode.emptyArray, pos);
    }

    /**
     * Creates a call to this operand with a single operand.
     */
    public SqlCall createCall(
        SqlNode operand,
        ParserPosition pos)
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
        ParserPosition pos)
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
        ParserPosition pos)
    {
        return createCall(
            new SqlNode [] { operand1, operand2, operand3 },
            pos);
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
     * Validate a call to this operator. Called just after the operands have been
     * validated.
     * @param call the SqlCall node for the call.
     * @param validator the active validator.
     */
    void validateCall(
        SqlCall call,
        SqlValidator validator)
    {
        return; // default is to do nothing
    }

    /**
     * Deduces the type of a call to this operator, assuming that the types of
     * the arguments are already known.
     *
     * <p>Particular operators can affect the behavior of this method in two
     * ways. If they have a {@link org.eigenbase.sql.type.ReturnTypeInference}, it is used; otherwise, they
     * must override this method. (Operators with unusual type inference schemes
     * should override this method; others should generally use a type-inference
     * strategy to share code.)
     */
    public RelDataType getType(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes)
    {
        if (typeInference != null) {
            return typeInference.getType(typeFactory, argTypes);
        }

        // If you see this you must either give typeInference a value
        // or override this method.
        throw Util.needToImplement(this);
    }

    /**
     * Deduces the type of a call to this operator, using information from the
     * operands.
     *
     * Normally (as in the default implementation), only the types of the operands
     * are needed  to determine the type of the call.  If the call type depends
     * on the values of the operands, then override this method.
     */
    public RelDataType getType(
        RelDataTypeFactory typeFactory,
        RexNode [] exprs)
    {
        return getType(
            typeFactory,
            TypeUtil.collectTypes(exprs));
    }

    /**
     * Deduces the type of a call to this operator. To do this, the method
     * first needs to recursively deduce the types of its arguments, using
     * the validator and scope provided.
     *
     * <p>Particular operators can affect the behavior of this method in two
     * ways. If they have a {@link org.eigenbase.sql.type.ReturnTypeInference}, it is used; otherwise, they
     * must override this method. (Operators with unusual type inference schemes
     * should override this method; others should generally use a type-inference
     * strategy to share code.)
     */
    public RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        // Check that there's the right number of arguments.
        checkNumberOfArg(operandsCheckingRule, call);

        checkArgTypes(call, validator, scope);

        // Now infer the result type.
        return inferType(validator, scope, call);
    }

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    protected void checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        if (null == operandsCheckingRule) {
            // If you see this you must either give operandsCheckingRule a value
            // or override this method.
            throw Util.needToImplement(this);
        }

        operandsCheckingRule.check(validator, scope, call, true);
    }

    protected boolean checkArgTypesNoThrow(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        try {
            checkArgTypes(call, validator, scope);
        } catch (RuntimeException e) {
            // todo, hack for now. Should not rely on catching exception, instead
            // refactor so that checkAryTypes and overriding methods returns a
            // boolean instead.
            Util.swallow(e, null);
            return false;
        }
        return true;
    }

    protected void checkNumberOfArg(
        OperandsTypeChecking argType,
        SqlCall call)
    {
        OperandsCountDescriptor od =
            call.operator.getOperandsCountDescriptor();
        if (!od.isVariadic()
                && !od.getPossibleNumOfOperands().contains(
                    new Integer(call.operands.length))) {
            throw EigenbaseResource.instance().newWrongNumOfArguments(
                "" + call,
                call.getParserPosition().toString());
        }
    }

    /**
     * Figure out the type of the return of this function.
     * We have already checked that the number and types of arguments are as
     * required.
     */
    protected RelDataType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        if (typeInference != null) {
            return typeInference.getType(validator, scope, call);
        }

        // Derived type should have overridden this method, since it didn't
        // supply a type inference rule.
        throw Util.needToImplement(this);
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

    // REVIEW jvs 23-Dec-2003:  need wrapper call like getType?
    public UnknownParamInference getParamTypeInference()
    {
        return paramTypeInference;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A class that describes how many operands a operator can take
     */
    public static class OperandsCountDescriptor
    {
        public static final OperandsCountDescriptor variadicCountDescriptor =
            new OperandsCountDescriptor();
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
