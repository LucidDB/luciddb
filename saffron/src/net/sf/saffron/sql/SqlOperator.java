/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.Util;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.text.MessageFormat;


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
    final int leftPrec;

    /**
     * The precedence with which this operator binds to the expression to the
     * right. This is more than the right precedence if the operator is
     * left-associative.
     */
    final int rightPrec;

    private final TypeInference typeInference;
    private final ParamTypeInference paramTypeInference;
    protected final AllowdArgInference argTypeInference;

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
        AllowdArgInference argTypeInference)
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
        AllowdArgInference argTypeInference)
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

    /** Returns how many operands this operator takes */
    public int getNumOfOperands() {
        return argTypeInference.getNbrOfArgs();
    }

    public String toString() {
        return name;
    }

    /**
     * Returns a template describing how the operator signature is to be built.
     * E.g for the binary + operator the template looks like "{1} {0} {2}"
     * {0} is the operator, subsequent nbrs are operands.
     * If null is returned, the default template will be used which is opname(operand0, operand1, ...)
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
    abstract void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec);

    /**
     * Returns whether this is a particular operator.
     */
    boolean isA(SqlKind kind)
    {
        // REVIEW jvs 6-Feb-2004:  why is this logic duplicated with
        // SqlKind.isA?
        switch (kind.getOrdinal()) {
        case SqlKind.SetQueryORDINAL:
            switch (this.kind.getOrdinal()) {
            case SqlKind.UnionORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.ExceptORDINAL:
                return true;
            default:
                return false;
            }
        case SqlKind.ExpressionORDINAL:
            switch (this.kind.getOrdinal()) {
            case SqlKind.AsORDINAL:
            case SqlKind.DescendingORDINAL:
            case SqlKind.SelectORDINAL:
            case SqlKind.JoinORDINAL:
            case SqlKind.FunctionORDINAL:
                return false;
            default:
                return true;
            }
        case SqlKind.DmlORDINAL:
            switch (this.kind.getOrdinal()) {
            case SqlKind.InsertORDINAL:
            case SqlKind.DeleteORDINAL:
            case SqlKind.UpdateORDINAL:
                return true;
            default:
                return false;
            }
        case SqlKind.QueryORDINAL:
            switch (this.kind.getOrdinal()) {
            case SqlKind.ExceptORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.SelectORDINAL:
            case SqlKind.UnionORDINAL:
            case SqlKind.OrderByORDINAL:
            case SqlKind.ValuesORDINAL:
            case SqlKind.ExplicitTableORDINAL:
                return true;
            default:
                return false;
            }
        default:
            return kind == this.kind;
        }
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
        return kind.getOrdinal()*31 + name.hashCode();
    }

    public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope,
            SqlCall call) {
        // Check that there's the right number of arguments.
        checkNumberOfArg(argTypeInference, call); //todo

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
        SqlCall call, SqlValidator validator, SqlValidator.Scope scope)
    {
        // Check that all of the arguments are of the right type, or are at
        // least assignable to the right type.
        argTypeInference.check(validator,scope,call);
    }

    protected void checkNumberOfArg(final AllowdArgInference argType, SqlCall call) {
        if (argType!=null && argType.getNbrOfArgs() != call.operands.length) {
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
     * Returns a string describing the actual argument types of a call, e.g.
     * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature(
        SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
        // REVIEW maybe move this method to SqlCall
        StringBuffer buf = new StringBuffer();
        ArrayList signatureList = new ArrayList();
        for (int i = 0; i < call.operands.length; i++) {
            final SqlNode operand = call.operands[i];
            final SaffronType argType =
                validator.deriveType(scope,operand);
            signatureList.add(argType.toString());
        }
        buf.append(getSignature(signatureList));
        return buf.toString();
    }

    /**
     * Returns a string describing the expected argument types of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    public String getAllowedSignatures() {
        return argTypeInference.getAllowedSignatures(this).trim();
    }

    private String getSignature(ArrayList list){
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


    public SaffronType getType(SaffronTypeFactory typeFactory, SaffronType[] argTypes) {
        if (typeInference != null) {
            return typeInference.getType(typeFactory, argTypes);
        }
        throw Util.needToImplement(this);
    }

    // REVIEW jvs 23-Dec-2003:  need wrapper call like getType?
    public ParamTypeInference getParamTypeInference()
    {
        return paramTypeInference;
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
    public static class AllowdArgInference
    {
        private SqlTypeName[][] m_types;

        public AllowdArgInference()
        {   //empty constructor
        }

        public AllowdArgInference(SqlTypeName[][] types)
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

        public void check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            assert(getNbrOfArgs()==call.operands.length);
            SaffronType anyType = validator.typeFactory.createSqlType(SqlTypeName.Any);

            //iterating over the operands...
            for(int i=0;i<call.operands.length;i++)
            {
                SqlNode operand = call.operands[i];
                SaffronType actualType = validator.deriveType(scope,operand);
                //for each operand, iterater over its allowed types...
                int foundAllowed=0;
                for (int j = 0; j < m_types[i].length; j++)
                {
                    SqlTypeName typeName = m_types[i][j];
                    SaffronType expectedType;
                    if (typeName.allowsNoPrecNoScale()){
                        expectedType = validator.typeFactory.createSqlType(typeName);
                    } else if (typeName.allowsPrecNoScale()){
                        expectedType = validator.typeFactory.createSqlType(typeName,0);
                    } else {
                        expectedType = validator.typeFactory.createSqlType(typeName,0,0);
                    }

                    assert(expectedType != null);
                    if(anyType.equals(expectedType)) {
                        // If the argument type is defined as any type, we don't need to check
                        foundAllowed++;
                        continue;
                    }
                    else if (expectedType.isAssignableFrom(actualType)){
                        foundAllowed++;
                        break;
                    }

                }
                //finished checking all allowed types for this operand
                if (0==foundAllowed)
                {
                    throw validator.newValidationError("Can not apply '"+call.operator.name+"' to arguments of type " +
                                                       call.operator.getCallSignature(validator, scope, call)+
                                                       ". Supported form(s): "
                                                       +call.operator.getAllowedSignatures());
                }

            }
        }

        int getNbrOfArgs() {
            return m_types.length;
        }

        SqlTypeName[][] getTypes(){
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

    public static class CompositeAllowdArgInference extends AllowdArgInference{
        private AllowdArgInference[] m_allowedRules;

        public CompositeAllowdArgInference(AllowdArgInference[] allowedRules) {
            Util.pre(null!=allowedRules,"null!=allowedRules");
            Util.pre(allowedRules.length>1,"Not a composite type");
            int firstArgsLength = allowedRules[0].getNbrOfArgs();
            for (int i = 1; i < allowedRules.length; i++) {
                Util.pre(allowedRules[i].getNbrOfArgs()==firstArgsLength,"All must have the same operand length");
            }
            m_allowedRules = allowedRules;
        }

        public String getAllowedSignatures(SqlOperator op) {
            StringBuffer ret = new StringBuffer();
            for (int i = 0; i < m_allowedRules.length; i++) {
                AllowdArgInference rule = m_allowedRules[i];
                if (i>0){
                    ret.append(NL);
                }
                ret.append(rule.getAllowedSignatures(op));
            }
            return ret.toString();
        }

        SqlTypeName[][] getTypes()
        {
            Util.newInternal("Should not be called");
            return null;
        }

        int getNbrOfArgs() {
            //check made in constructor to verify that all rules have the same nbrOfArgs
            //take and return the first one
            return m_allowedRules[0].getNbrOfArgs();
        }

        public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call) {
            String[] typeErrors = new String[m_allowedRules.length];
            int nbrOfTypeErrors = 0;

            for (int i = 0; i < m_allowedRules.length; i++) {
                AllowdArgInference rule = m_allowedRules[i];
                try {
                    typeErrors[i] = null;
                    rule.check(validator,scope,call);
                } catch (RuntimeException e) {
                    typeErrors[i] = e.getMessage();
                    nbrOfTypeErrors++;
                }
            }

            if (nbrOfTypeErrors==m_allowedRules.length) {
                StringBuffer msg = new StringBuffer();
                for (int i = 0; i < typeErrors.length; i++) {
                    String typeError = typeErrors[i];
                    if (null==typeError) {
                        continue;
                    }
                    if (msg.length()!=0){
                        msg.append(" or ");
                    }
                    msg.append(typeError);
                }

                throw validator.newValidationError(msg.toString());
            }
        }
    }
}


// End SqlOperator.java
