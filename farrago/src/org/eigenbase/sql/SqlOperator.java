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

import java.text.MessageFormat;
import java.util.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validation.ValidationUtil;
import org.eigenbase.util.Util;


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
    private final TypeInference typeInference;

    /** used to inference unknown params */
    private final ParamTypeInference paramTypeInference;

    /** used to validate operands */
    protected final AllowedArgInference argTypeInference;

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
        TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference argTypeInference)
    {
        Util.pre(kind != null, "kind != null");

//        Util.pre(argTypeInference != null, "argTypeInference != null");
        this.name = name;
        this.kind = kind;
        this.leftPrec = leftPrecedence;
        this.rightPrec = rightPrecedence;
        this.typeInference = typeInference;
        this.paramTypeInference = paramTypeInference;
        this.argTypeInference = argTypeInference;
    }

    /**
     * Creates an operator specifying left/right associativity.
     */
    protected SqlOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean isLeftAssoc,
        TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference argTypeInference)
    {
        this(name, kind, (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0), typeInference,
            paramTypeInference, argTypeInference);
    }

    //~ Methods ---------------------------------------------------------------

    public AllowedArgInference getAllowedArgInference()
    {
        return this.argTypeInference;
    }

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        if (null != argTypeInference) {
            return new OperandsCountDescriptor(argTypeInference.getArgCount());
        }

        // If you see this error you need to overide this method
        // or give argTypeInference a value.
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
     * An abstract method where its implementations call the
     * {@link org.eigenbase.sql.test.SqlTester}'s
     * different <code>checkXXX</code> methods.
     * An example test function for the sin operator
     * <blockqoute><pre><code>
     * void test(SqlTester tester) {<br>
     *     tester.checkScalar("sin(0)", "0");<br>
     *     tester.checkScalar("sin(1.5707)", "1");<br>
     * }<br>
     * </code></pre></blockqoute>
     * @param tester The tester to use.
     */
    public abstract void test(SqlTester tester);

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
        return createCall(
            new SqlNode[0],
            pos);
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
     */
    public abstract void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec);

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
     * ways. If they have a {@link TypeInference}, it is used; otherwise, they
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
            ValidationUtil.collectTypes(exprs));
    }

    /**
     * Deduces the type of a call to this operator. To do this, the method
     * first needs to recursively deduce the types of its arguments, using
     * the validator and scope provided.
     *
     * <p>Particular operators can affect the behavior of this method in two
     * ways. If they have a {@link TypeInference}, it is used; otherwise, they
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
        checkNumberOfArg(argTypeInference, call);

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
        if (null == argTypeInference) {
            // If you see this you must either give argTypeInference a value
            // or override this method.
            throw Util.needToImplement(this);
        }

        argTypeInference.check(validator, scope, call);
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
        AllowedArgInference argType,
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
        assert (null != argTypeInference) : "If you see this, assign argTypeInference a value "
        + "or override this function";
        return replaceAnonymous(
            argTypeInference.getAllowedSignatures(this),
            opNameToUse).trim();
    }

    /**
     * Returns the same as {@link #getAnonymousSignature} with the exception that
     * {@link this.name} is the function/operator signature string
     * @param list
     * @return
     */
    protected String getSignature(final ArrayList list)
    {
        return replaceAnonymous(
            getAnonymousSignature(list),
            name);
    }

    /**
     * Returns a string of all allowed types, permutated with an anonymous
     * string represented by {@link #ANONYMOUS_REPLACE}.
     */
    protected String getAnonymousSignature(final ArrayList typeList)
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
    public ParamTypeInference getParamTypeInference()
    {
        return paramTypeInference;
    }

    public void isCharTypeComparableThrows(RelDataType [] argTypes)
    {
        if (!ValidationUtil.isCharTypeComparable(argTypes)) {
            String msg = "";
            for (int i = 0; i < argTypes.length; i++) {
                if (i > 0) {
                    msg += ", ";
                }
                RelDataType argType = argTypes[i];
                msg += argType.toString();
            }
            throw EigenbaseResource.instance().newTypeNotComparableEachOther(msg);
        }
    }

    public void isCharTypeComparableThrows(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlNode [] operands)
    {
        if (!ValidationUtil.isCharTypeComparable(validator, scope, operands)) {
            String msg = "";
            for (int i = 0; i < operands.length; i++) {
                if (i > 0) {
                    msg += ", ";
                }
                msg += operands[i].toString();
            }
            throw EigenbaseResource.instance().newOperandNotComparable(msg);
        }
    }

    //~ Inner Interfaces ------------------------------------------------------

    /**
     * Strategy to transform one type to another. The transformation is dependent on
     * the implemented strategy object and in the general case is a function of
     * the type and the other operands
     *
     * Can not be used by itself. Must be used in an object of type
     * {@link CascadeTypeInference}

     * <p>This class is an example of the
     * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
     * This makes sense because many operators have similar, straightforward
     * strategies, such as to take the type of the first operand.</p>
     */
    public interface TypeInferenceTransform
    {
        /**
         * @param typeToTransform The type subject of transformation. The return
         * type is (in the general case) a function of
         * <ul><li>The typeToTransform</li><li>The other operand types</li></ul>
         * {@link TypeInference}  object.
         * @return A new type depending on
         * {@param typeToTransform} and {@param argTypes}
         */
        RelDataType getType(
            RelDataTypeFactory typeFactory,
            RelDataType [] argTypes,
            RelDataType typeToTransform);
    }

    /**
     * Strategy to infer unknown types of the operands of an operator call.
     */
    public interface ParamTypeInference
    {
        /**
         * Infer any unknown operand types.
         *
         * @param validator the validator context
         *
         * @param scope the the validator scope context
         *
         * @param call the call being analyzed
         *
         * @param returnType the type known or inferred for the
         * result of the call
         *
         * @param operandTypes receives the inferred types for all operands
         */
        void inferOperandTypes(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            RelDataType returnType,
            RelDataType [] operandTypes);
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

        private OperandsCountDescriptor()
        {
            possibleList = null;
            isVariadic = true;
        }

        public OperandsCountDescriptor(int count)
        {
            possibleList = Collections.unmodifiableList(
                Arrays.asList(new Object [] { new Integer(count) }));
            isVariadic = false;
        }

        public OperandsCountDescriptor(
            int count1,
            int count2)
        {
            possibleList = Collections.unmodifiableList(Arrays.asList(
                  new Object [] { new Integer(count1), new Integer(count2) }));
            isVariadic = false;
        }

        public OperandsCountDescriptor(
            int count1,
            int count2,
            int count3)
        {
            possibleList = Collections.unmodifiableList(
                Arrays.asList(
                    new Object [] {
                        new Integer(count1), new Integer(count2),
                        new Integer(count3)
                    }));
            isVariadic = false;
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

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands.
     *
     * <p>This class is an example of the
     * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
     * This makes sense because many operators have similar, straightforward
     * strategies, such as to take the type of the first operand.</p>
     */
    public static abstract class TypeInference
    {
        // REVIEW jvs 26-May-2004:  I think we should try to eliminate one
        // of these methods; they are redundant.
        public abstract RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call);

        public abstract RelDataType getType(
            RelDataTypeFactory typeFactory,
            RelDataType [] argTypes);

        /**
         * Iterates over all of the call's operands and derive their types
         * before calling and returning the result from
         * {@link #getType(org.eigenbase.reltype.RelDataTypeFactory, org.eigenbase.reltype.RelDataType[])}
         */
        protected final RelDataType collectTypesFromCall(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            return getType(
                validator.typeFactory,
                ValidationUtil.collectTypes(validator, scope, call.operands));
        }
    }

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands by using one {@link TypeInference} rule and a combination of
     * {@link TypeInferenceTransform}s
     */
    public static class CascadeTypeInference extends TypeInference
    {
        final TypeInference rule;
        final TypeInferenceTransform [] transforms;

        /**
         * Creates a CascadeTypeInference from a rule and an array of one or
         * more transforms.
         *
         * @pre null!=rule
         * @pre null!=transforms
         * @pre transforms.length > 0
         * @pre transforms[i] != null
         */
        public CascadeTypeInference(
            TypeInference rule,
            TypeInferenceTransform [] transforms)
        {
            Util.pre(null != rule, "null!=rule");
            Util.pre(null != transforms, "null!=transforms");
            Util.pre(transforms.length > 0, "transforms.length>0");
            for (int i = 0; i < transforms.length; i++) {
                Util.pre(transforms[i] != null, "transforms[i] != null");
            }
            this.rule = rule;
            this.transforms = transforms;
        }

        /**
         * Creates a CascadeTypeInference from a rule and a single transform.
         *
         * @pre null!=rule
         * @pre null!=transform
         */
        public CascadeTypeInference(
            TypeInference rule,
            TypeInferenceTransform transform)
        {
            this(rule, new TypeInferenceTransform [] { transform });
        }

        /**
         * Creates a CascadeTypeInference from a rule and two transforms.
         *
         * @pre null!=rule
         * @pre null!=transform0
         * @pre null!=transform1
         */
        public CascadeTypeInference(
            TypeInference rule,
            TypeInferenceTransform transform0,
            TypeInferenceTransform transform1)
        {
            this(rule, new TypeInferenceTransform [] { transform0, transform1 });
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            return collectTypesFromCall(validator, scope, call);
        }

        public RelDataType getType(
            RelDataTypeFactory typeFactory,
            RelDataType [] argTypes)
        {
            RelDataType ret = rule.getType(typeFactory, argTypes);
            for (int i = 0; i < transforms.length; i++) {
                TypeInferenceTransform transform = transforms[i];
                ret = transform.getType(typeFactory, argTypes, ret);
            }
            return ret;
        }
    }

    /**
     * Strategy to check for allowed operand types of an operator call.
     */
    public static class AllowedArgInference
    {
        protected SqlTypeName [][] types;

        public AllowedArgInference()
        { //empty constructor
        }

        public AllowedArgInference(SqlTypeName [][] types)
        {
            Util.pre(null != types, "null!=types");
            Util.pre(types.length > 0, "types.length>0");

            //only Null types specified? Prohibit! need more than null
            for (int i = 0; i < types.length; i++) {
                Util.pre(types[i].length > 0, "Need to define a type");
                boolean foundOne = false;
                for (int j = 0; j < types[i].length; j++) {
                    SqlTypeName sqlType = types[i][j];
                    if (!sqlType.equals(SqlTypeName.Null)) {
                        foundOne = true;
                        break;
                    }
                }

                if (!foundOne) {
                    Util.pre(false, "Must have at least one non-null type");
                }
            }

            this.types = types;
        }

        /**
         * Calls {@link #check(SqlCall, SqlValidator,SqlValidator.Scope,SqlNode,int)}
         * with {@param node}
         * @param node one of the operands of {@param call}
         * @param operandOrdinal
         * @pre call != null
         */
        public void checkThrows(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            SqlNode node,
            int operandOrdinal)
        {
            Util.pre(null != call, "null != call");
            if (!check(call, validator, scope, node, operandOrdinal)) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }

        /**
         * Checks if a node is of correct type
         * @param call
         * @param validator
         * @param scope
         * @param node
         * @param operandOrdinal
         *
         * Note that <code>operandOrdinal</code> is <i>not</i> an index in any
         * call.operands[] array. It's rather used to specify which
         * signature the node should correspond too.
         *
         * <p>For example, if we have typeStringInt, a check can be made to see
         * if a <code>node</code> is of type int by calling
         * <code>typeStringInt.check(validator,scope,node,1);</code>
         */
        public boolean check(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlNode node,
            int operandOrdinal)
        {
            RelDataType anyType =
                validator.typeFactory.createSqlType(SqlTypeName.Any);
            RelDataType actualType = null;

            //for each operand, iterater over its allowed types...
            for (int j = 0; j < types[operandOrdinal].length; j++) {
                SqlTypeName typeName = types[operandOrdinal][j];
                RelDataType expectedType =
                    RelDataTypeFactoryImpl.createSqlTypeIgnorePrecOrScale(validator.typeFactory,
                        typeName);
                assert (expectedType != null);
                if (anyType.equals(expectedType)) {
                    // If the argument type is defined as any type, we don't need to check
                    return true;
                } else {
                    if( null == actualType) {
                        actualType = validator.deriveType(scope, node);
                    }

                    if (expectedType.isAssignableFrom(actualType, false)) {
                        return true;
                    }
                }
            }
            return false;
        }

        public void check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            if (!checkNoThrowing(call, validator, scope)) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }

        public boolean checkNoThrowing(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope)
        {
            assert (getArgCount() == call.operands.length);

            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];
                if (!check(call, validator, scope, operand, i)) {
                    return false;
                }
            }
            return true;
        }

        public int getArgCount()
        {
            return types.length;
        }

        public SqlTypeName [][] getTypes()
        {
            return types;
        }

        /**
         * Returns a string describing the expected argument types of a call, e.g.
         * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
         */
        public String getAllowedSignatures(SqlOperator op)
        {
            StringBuffer buf = new StringBuffer();
            ArrayList list = new ArrayList();
            getAllowedSignatures(0, list, buf, op);
            return buf.toString().trim();
        }

        /**
         * Helper function to {@link #getAllowedSignatures(SqlOperator)}
         */
        protected void getAllowedSignatures(
            int depth,
            ArrayList list,
            StringBuffer buf,
            SqlOperator op)
        {
            assert (null != types[depth]);
            assert (types[depth].length > 0);

            for (int i = 0; i < types[depth].length; i++) {
                SqlTypeName type = types[depth][i];
                if (type.equals(SqlTypeName.Null)) {
                    continue;
                }

                list.add(type);
                if ((depth + 1) < types.length) {
                    getAllowedSignatures(depth + 1, list, buf, op);
                } else {
                    buf.append(op.getAnonymousSignature(list));
                    buf.append(NL);
                }
                list.remove(list.size() - 1);
            }
        }
    }

    /**
     * This class allows multiple existing {@link #AllowedArgInference} rules
     * to be combined into one rule.<p>
     * For example, giving an operand the signature of both a string and a numeric
     * could be done by:
     * <blockquote><pre><code>
     *
     * CompositeAllowedArgInference newCompositeRule =
     *  new SqlOperator.CompositeAllowedArgInference(
     *    new SqlOperator.AllowedArgInference[]{stringRule, numericRule});
     *
     * </code></pre></blockquote>
     */
    public static class CompositeAllowedArgInference
        extends AllowedArgInference
    {
        private AllowedArgInference [] allowedRules;

        public CompositeAllowedArgInference(
            AllowedArgInference [] allowedRules)
        {
            Util.pre(null != allowedRules, "null != allowedRules");
            Util.pre(allowedRules.length > 1, "Not a composite type");
            int firstArgsLength = allowedRules[0].getArgCount();
            for (int i = 1; i < allowedRules.length; i++) {
                Util.pre(allowedRules[i].getArgCount() == firstArgsLength,
                    "All must have the same operand length");
            }
            this.allowedRules = allowedRules;
        }

        public AllowedArgInference [] getRules()
        {
            return allowedRules;
        }

        public String getAllowedSignatures(SqlOperator op)
        {
            StringBuffer ret = new StringBuffer();
            for (int i = 0; i < allowedRules.length; i++) {
                AllowedArgInference rule = allowedRules[i];
                if (i > 0) {
                    ret.append(NL);
                }
                ret.append(rule.getAllowedSignatures(op));
            }
            return ret.toString();
        }

        public SqlTypeName [][] getTypes()
        {
            throw Util.newInternal("Should not be called");
        }

        public int getArgCount()
        {
            // Check made in constructor to verify that all rules have the same
            // number of arguments. Take and return the first one.
            return allowedRules[0].getArgCount();
        }

        public boolean check(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlNode node,
            int operandOrdinal)
        {
            Util.pre(allowedRules.length >= 1, "allowedRules.length>=1");
            for (int i = 0; i < allowedRules.length; i++) {
                AllowedArgInference rule = allowedRules[i];
                if (rule.check(call, validator, scope, node, operandOrdinal)) {
                    return true;
                }
            }
            return false;
        }

        public void check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            int typeErrorCount = 0;

            for (int i = 0; i < allowedRules.length; i++) {
                AllowedArgInference rule = allowedRules[i];

                if (!rule.checkNoThrowing(call, validator, scope)) {
                    typeErrorCount++;
                }
            }

            if (typeErrorCount == allowedRules.length) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }
    }
}


// End SqlOperator.java
