/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.sql;

import net.sf.saffron.calc.RexToCalcTranslator;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.Util;
import openjava.ptree.Expression;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>SqlOperator</code> is a type of node in a SQL parse tree (it is NOT
 * a node in a SQL parse tree). It includes functions, operators such as '=',
 * and syntactic constructs such as 'case' statements.
 */
public abstract class SqlOperator
{
    //~ Instance fields -------------------------------------------------------

    public String name;
    public final SqlKind kind;

    /**
     * The precedence with which this operator binds to the expression to the
     * left. This is less than the right precedence if the operator is
     * left-associative.
     */
    public final int leftPrec;

    /**
     * The precedence with which this operator binds to the expression to the
     * right. This is more than the right precedence if the operator is
     * left-associative.
     */
    public final int rightPrec;

    /** used to get the return type of operator/function */
    private final TypeInference typeInference;
    /** used to inference unknown params */
    private final ParamTypeInference paramTypeInference;
    /** used to validate operands */
    protected final AllowedArgInference argTypeInference;

    public static final String NL = System.getProperty("line.separator");
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an operator.
     *
     * @pre kind != null
     */
    // @pre paramTypes != null
    SqlOperator(
        String name,SqlKind kind,int leftPrecedence,int rightPrecedence,
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
    SqlOperator(
        String name,SqlKind kind,int prec,boolean isLeftAssoc,
        TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference argTypeInference)
    {
        this(
            name,
            kind,
            (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0),
            typeInference,
            paramTypeInference, argTypeInference);
    }

    //~ Methods ---------------------------------------------------------------

    public AllowedArgInference getAllowedArgInference() {
        return this.argTypeInference;
    }

    public List getPossibleNumOfOperands() {
        if (null!=argTypeInference) {
            List ret = new ArrayList(argTypeInference.getArgCount());
            ret.add(new Integer(argTypeInference.getArgCount()));
            return ret;
        }

        throw Util.needToImplement(
                "if you see this you must either give argTypeInference a value " +
                "or override this method for Operator "+name);
    }

    /**
     * Returns how many operands this operator takes.
     * See also {@link #getPossibleNumOfOperands}.
     * @param desiredCount Indicates how many operands the caller would like
     * to have, and can be ignored depending on the operator.
     * This parameter can be used to return the right value in case
     * the operator can take a different number of operands.
     * See {@link #getPossibleNumOfOperands}
     * @return the number of operands this operator can take.
     * Note, this value is not fixed. Depending on the operator and the input
     * and how it was created (i.e. how many operands it was created with)
     * it may return a different value.
     * See {@link #getPossibleNumOfOperands}
     */
    public int getNumOfOperands(int desiredCount) {
        Util.discard(desiredCount);
        if (null==argTypeInference) {
            Util.needToImplement(
                    "If you see this you need to overide this method"+
                    "or give argTypeInference a value");
        }
        return argTypeInference.getArgCount();
    }

    public String toString() {
        return name;
    }

    /**
     * Returns a template describing how the operator signature is to be built.
     * E.g for the binary + operator the template looks like "{1} {0} {2}"
     * {0} is the operator, subsequent nbrs are operands.
     * If null is returned, the default template will be used which
     * is opname(operand0, operand1, ...)
     */
    protected String getSignatureTemplate() {
        return null;
    }

    /**
     * Returns the syntactic type of this operator, a value from {@link
     * SqlOperator.Syntax}.
     */
    public abstract int getSyntax();

    /**
     * An abstract method where its implementations call the
     * {@link net.sf.saffron.sql.test.SqlTester}'s
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
    public SqlCall createCall(SqlNode [] operands)
    {
        return new SqlCall(this,operands);
    }

    /**
     * Creates a call to this operand with no operands.
     */
    public SqlCall createCall()
    {
        return createCall(new SqlNode[0]);
    }

    /**
     * Creates a call to this operand with a single operand.
     */
    public SqlCall createCall(SqlNode operand)
    {
        return createCall(new SqlNode [] { operand });
    }

    /**
     * Creates a call to this operand with two operands.
     */
    public SqlCall createCall(SqlNode operand1,SqlNode operand2)
    {
        return createCall(new SqlNode [] { operand1,operand2 });
    }

    /**
     * Creates a call to this operand with three operands.
     */
    public SqlCall createCall(
        SqlNode operand1,SqlNode operand2,SqlNode operand3)
    {
        return createCall(new SqlNode [] { operand1,operand2,operand3 });
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
        return kind.getOrdinal()*31 + name.hashCode();
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
    public SaffronType getType(SaffronTypeFactory typeFactory, SaffronType[] argTypes) {
        if (typeInference != null) {
            return typeInference.getType(typeFactory, argTypes);
        }
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
    public SaffronType getType(SaffronTypeFactory typeFactory, RexNode[] exprs) {
        return getType(typeFactory, getTypes(exprs));
    }

    private SaffronType[] getTypes(RexNode[] exprs) {
        SaffronType[] types = new SaffronType[exprs.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = exprs[i].getType();
        }

        return types;
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
    public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope,
                               SqlCall call) {
        // Check that there's the right number of arguments.
        checkNumberOfArg(call);

        checkArgTypes(call, validator, scope);

        // Now infer the result type.
        return inferType(validator, scope, call);
    }

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        if (null==argTypeInference) {
            throw Util.needToImplement(
                "if you see this you must either give argTypeInference a value"+
                " or override this method");

        }

        argTypeInference.check(validator,scope,call);
    }

    protected void checkNumberOfArg(final AllowedArgInference argType, SqlCall call) {
        boolean doThrow = false;
        if (argType!=null) {
            doThrow = argType.getArgCount() != call.operands.length;
        } else {
            doThrow = call.operator.getNumOfOperands(call.operands.length)!=
                            call.operands.length;
        }

        if (doThrow) {
            throw SaffronResource.instance().newValidationError(
                                    "Wrong number of arguments to " + call);
        }
    }

    protected void checkNumberOfArg(SqlCall call) {
        assert argTypeInference != null : "Derived class should have set " +
                "argTypeInference or overridden this method";
        if (argTypeInference.getArgCount() != call.operands.length) {
            throw Util.newInternal("todo: Wrong number of arguments to " + call);
        }
    }

    /**
     * Figure out the type of the return of this function.
     * We have already checked that the number and types of arguments are as
     * required.
     */
    protected SaffronType inferType(SqlValidator validator,
                                    SqlValidator.Scope scope, SqlCall call) {
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
    public String getAllowedSignatures() {
        assert(null!=argTypeInference) :
                "If you see this, assign argTypeInference a value " +
                "or override this function";
        return argTypeInference.getAllowedSignatures(this).trim();
    }

    protected String getSignature(final ArrayList list){
        StringBuffer ret = new StringBuffer();
        String template = getSignatureTemplate();
        if (null==template) {
            ret.append("'");
            ret.append(name);
            ret.append("(");
            for (int i = 0; i < list.size(); i++) {
                if (i>0) {
                    ret.append(", ");
                }
                ret.append("<"+list.get(i).toString()+">");
            }
            ret.append(")'");
        }
        else {
            Object[] values = new Object[list.size()+1];
            values[0] = this.name;
            ret.append("'");
            for (int i = 0; i <list.size(); i++) {
                values[i+1] = "<"+list.get(i)+">";
            }
            ret.append(MessageFormat.format(template, values));
            ret.append("'");
            assert((list.size()+1)==values.length);
        }

        return ret.toString();
    }

    // REVIEW jvs 23-Dec-2003:  need wrapper call like getType?
    public ParamTypeInference getParamTypeInference()
    {
        return paramTypeInference;
    }

    /**
     * Checks if two types or more are char comparable.
     * @pre argTypes != null
     * @pre argTypes.length >= 2
     * @return Returns true if all operands are of char type
     *         and if they are comparable, i.e. charset and collation agree
     */
    public boolean isCharTypeComparable(SaffronType[] argTypes){
        Util.pre(null!=argTypes,"null!=operands");
        Util.pre(2<=argTypes.length,"1<=operands.length");

        for (int j=0;j<(argTypes.length-1);j++){
             SaffronType t0 = argTypes[j];
             SaffronType t1 = argTypes[j+1];

            if (!t0.isCharType() && !t1.isCharType()) {
                return false;
            }

             if (null==t0.getCharset()) {
                 Util.newInternal(
                    "SaffronType object should have been assigned a " +
                    "(default) charset when calling deriveType");
            } else if (!t0.getCharset().equals(t1.getCharset())) {
                return false;
            }

            if (null==t0.getCollation()) {
                Util.newInternal(
                    "SaffronType object should have been assigned a " +
                    "(default) collation when calling deriveType");
            } else if (!t0.getCollation().equals(t1.getCollation())) {
                return false;
            }
        }

        return true;
    }

     public void isCharTypeComparableThrows(SaffronType[] argTypes){
        if (!isCharTypeComparable(argTypes)) {
            String msg="Types ";
            for (int i = 0; i < argTypes.length; i++) {
                if (i>0) {
                    msg+=", ";
                }
                SaffronType argType = argTypes[i];
                msg+=argType.toString();
            }
            msg+=" not comparable to eachother";
            throw SaffronResource.instance().newValidationError(msg);
        }
     }

    public void isCharTypeComparableThrows(SqlValidator validator,
                                           SqlValidator.Scope scope,
                                           SqlNode[] operands) {
        if (!isCharTypeComparable(validator,scope,operands)) {
            String msg="Operands ";
            for (int i = 0; i < operands.length; i++) {
                if (i>0) {
                    msg+=", ";
                }
                msg+=operands[i].toString();
            }
            msg+=" not comparable to eachother";
            throw SaffronResource.instance().newValidationError(msg);
        }
    }

    /**
     * @param start zero based index
     * @param stop zero based index
     */
    public boolean isCharTypeComparable(SaffronType[] argTypes,
                                        int start, int stop){
        int n = stop-start+1;
        SaffronType[] subset = new SaffronType[n];
        System.arraycopy(argTypes, start, subset, 0, n );
        return isCharTypeComparable(subset);
    }

    public boolean isCharTypeComparable(SqlValidator validator,
                                           SqlValidator.Scope scope,
                                           SqlNode[] operands) {
        Util.pre(null!=operands,"null!=operands");
        Util.pre(2<=operands.length,"1<=operands.length");

        SaffronType[] argTypes = new SaffronType[operands.length];
        for (int i = 0; i < operands.length; i++) {
            argTypes[i] = validator.deriveType(scope, operands[i]);
        }

        return isCharTypeComparable(argTypes);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * <code>Syntax</code> enumerates possible syntactic types of operators.
     */
    public static class Syntax extends EnumeratedValues
    {
        public static final Syntax instance = new Syntax();
        public static final int Function = 0;
        public static final int Binary = 1;
        public static final int Prefix = 2;
        public static final int Postfix = 3;
        public static final int Special = 4;

        private Syntax()
        {
            super(
                new String [] { "function","binary","prefix","postfix","special" });
        }
    }

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands.
     *
     * <p>This class is an example of the
     * {@link net.sf.saffron.util.Glossary#StrategyPattern strategy pattern}.
     * This makes sense because many operators have similar, straightforward
     * strategies, such as to take the type of the first operand.</p>
     */
    public interface TypeInference {
        // REVIEW jvs 26-May-2004:  I think we should try to eliminate one
        // of these methods; they are redundant.

        SaffronType getType(SqlValidator validator, SqlValidator.Scope scope,
                SqlCall call);
        SaffronType getType(SaffronTypeFactory typeFactory,
                SaffronType[] argTypes);
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
            SaffronType returnType,
            SaffronType [] operandTypes);
    }

    /**
     * Strategy to check for allowed operand types of an operator call.
     */
    public static class AllowedArgInference
    {
        protected SqlTypeName[][] m_types;

        public AllowedArgInference()
        {   //empty constructor
        }

        public AllowedArgInference(SqlTypeName[][] types)
        {
            Util.pre(null!=types,"null!=types");
            Util.pre(types.length>0,"types.length>0");

            //only Null types specified? Prohibit! need more than null
            for (int i = 0; i < types.length; i++)
            {
                Util.pre(types[i].length>0,"Need to define a type");
                boolean foundOne=false;
                for (int j = 0; j < types[i].length; j++) {
                    SqlTypeName sqlType = types[i][j];
                    if (!sqlType.equals(SqlTypeName.Null)){
                        foundOne=true;
                        break;
                    }
                }

                if (!foundOne)
                {
                    Util.pre(false,"Must have at least one non-null type");
                }
            }


            this.m_types=types;
        }

        /**
         * Calls {@link #check(SqlCall, SqlValidator,SqlValidator.Scope,SqlNode,int)}
         * with {@param node}
         * @param node one of the operands of {@param call}
         * @param operandNbr
         * @pre call!=null
         */
        public void checkThrows(SqlValidator validator, SqlValidator.Scope scope,
                                SqlCall call, SqlNode node, int operandNbr) {
            Util.pre(null!=call,"null!=call");
            if (!check(call,validator,scope,node,operandNbr)){
                throw call.newValidationSignatureError(validator, scope);
            }
        }


        /**
         * Checks if a node is of correct type
         * @param call
         * @param validator
         * @param scope
         * @param node
         * @param operandNbr Note this is <i>not</i> an index in any call.operands[] array.
         *        Its rather used to specify which signature the node should correspond too.
         * <p>Example. if we have typeStringInt<br>
         * a check can be made to see if a <code>node</code> is of type int by calling
         * <code>typeStringInt.check(validator,scope,node,1);</code>
         */
        public boolean check(SqlCall call, SqlValidator validator, SqlValidator.Scope scope,
                SqlNode node, int operandNbr)
        {
            int foundAllowed=0;
            SaffronType anyType = validator.typeFactory.createSqlType(SqlTypeName.Any);
            SaffronType actualType = validator.deriveType(scope,node);

            //for each operand, iterater over its allowed types...
            for (int j = 0; j < m_types[operandNbr].length; j++)
            {
                SqlTypeName typeName = m_types[operandNbr][j];
                SaffronType expectedType =
                        SaffronTypeFactoryImpl.createSqlTypeIgnorePrecOrScale(validator.typeFactory, typeName);
                assert(expectedType != null);
                if(anyType.equals(expectedType)) {
                    // If the argument type is defined as any type, we don't need to check
                    foundAllowed++;
                    continue;
                }
                else if (expectedType.isAssignableFrom(actualType, false)){
                    foundAllowed++;
                    break;
                }
            }
            return foundAllowed>0;
        }

        public void check(SqlValidator validator, SqlValidator.Scope scope, SqlCall call)
        {
            if (!check(call, validator, scope)) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }

        public boolean check(SqlCall call, SqlValidator validator,
                             SqlValidator.Scope scope) {
            assert(getArgCount()==call.operands.length);

            for(int i=0;i<call.operands.length;i++)
            {
                SqlNode operand = call.operands[i];
                if (!check(call,validator,scope,operand,i))
                {
                    return false;
                }
            }
            return true;
        }

        public int getArgCount() {
            return m_types.length;
        }

        public SqlTypeName[][] getTypes(){
            return m_types;
        }

        /**
         * Returns a string describing the expected argument types of a call, e.g.
         * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
         */
        public String getAllowedSignatures(SqlOperator op) {
            StringBuffer buf = new StringBuffer();
            ArrayList list = new ArrayList();
            getAllowedSignatures(0,list,buf, op);
            return buf.toString().trim();
        }

        /**
         * Helper function to {@link #getAllowedSignatures(SqlOperator)}
         */
        protected void getAllowedSignatures(int depth, ArrayList list,StringBuffer buf,SqlOperator op) {
            assert(null!=m_types[depth]);
            assert(m_types[depth].length>0);

            for (int i = 0; i < m_types[depth].length; i++)
            {
                SqlTypeName type = m_types[depth][i];
                if (type.equals(SqlTypeName.Null)){
                    continue;
                }

                list.add(type);
                if (depth+1<m_types.length)
                {
                    getAllowedSignatures(depth+1,list,buf,op);
                }
                else
                {
                    buf.append(op.getSignature(list));
                    buf.append(NL);
                }
                list.remove(list.size()-1);
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
    public static class CompositeAllowedArgInference extends AllowedArgInference{
        private AllowedArgInference[] m_allowedRules;

        public CompositeAllowedArgInference(AllowedArgInference[] allowedRules) {
            Util.pre(null!=allowedRules,"null!=allowedRules");
            Util.pre(allowedRules.length>1,"Not a composite type");
            int firstArgsLength = allowedRules[0].getArgCount();
            for (int i = 1; i < allowedRules.length; i++) {
                Util.pre(allowedRules[i].getArgCount()==firstArgsLength,"All must have the same operand length");
            }
            m_allowedRules = allowedRules;
        }

        public String getAllowedSignatures(SqlOperator op) {
            StringBuffer ret = new StringBuffer();
            for (int i = 0; i < m_allowedRules.length; i++) {
                AllowedArgInference rule = m_allowedRules[i];
                if (i>0){
                    ret.append(NL);
                }
                ret.append(rule.getAllowedSignatures(op));
            }
            return ret.toString();
        }

        public SqlTypeName[][] getTypes()
        {
            Util.newInternal("Should not be called");
            return null;
        }

        public int getArgCount() {
            //check made in constructor to verify that all rules have the same nbrOfArgs
            //take and return the first one
            return m_allowedRules[0].getArgCount();
        }

        public boolean check(SqlCall call, SqlValidator validator,
                             SqlValidator.Scope scope, SqlNode node, int operandNbr) {
            Util.pre(m_allowedRules.length>=1,"m_allowedRules.length>=1");
            for (int i = 0; i < m_allowedRules.length; i++) {
                AllowedArgInference rule = m_allowedRules[i];
                if(rule.check(call, validator,scope, node,operandNbr)) {
                    return true;
                }
            }
            return false;
        }

        public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call) {
            int nbrOfTypeErrors = 0;

            for (int i = 0; i < m_allowedRules.length; i++) {
                AllowedArgInference rule = m_allowedRules[i];

                if (!rule.check(call,validator,scope))
                {
                    nbrOfTypeErrors++;
                }
            }

            if (nbrOfTypeErrors==m_allowedRules.length) {
                throw validator.newValidationError(call.getValidationSignatureErrorString(validator, scope));
            }
        }
    }
}


// End SqlOperator.java
