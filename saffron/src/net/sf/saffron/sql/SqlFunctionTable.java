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

import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.resource.SaffronResource;

import java.lang.reflect.Field;
import java.util.*;
import java.math.BigInteger;

/**
 * A <code>SqlFunctionTable</code> is a singleton which contains an instance of
 * each SQL built-in function.
 *
 * <p>REVIEW (jhyde): We should combine SqlFunctionTable with {@link
 * SqlOperatorTable}. The main difference is that SqlFunctionTable handles
 * overloading: but SqlOperatorTable will at some point need to handle
 * overloaded prefix and infix operators, so the difference will disappear.
 * Some of the functions may need to be renamed: {@link #lookup} may become
 * <code>lookupWithOverloading</code>.</p>
 *
 * @author kinkoi
 * @since Jan 05, 2004
 * @version $Id$
 **/
public class SqlFunctionTable {
    //~ Inner Classes -------------------------------------------------------
    /**
     * Enumerates the types of flags
     */
    public static class FunctionFlagType extends EnumeratedValues.BasicValue
    {
        private FunctionFlagType(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final FunctionFlagType Both =
                                        new FunctionFlagType("Both", 0);
        public static final FunctionFlagType Leading =
                                        new FunctionFlagType("Leading", 1);
        public static final FunctionFlagType Trailing =
                                        new FunctionFlagType("Trailing", 2);

        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new FunctionFlagType [] { Both,Leading,Trailing });

    }
    //~ Static fields/initializers --------------------------------------------

    public static final SqlLiteral flagBoth     =
            new SqlLiteral(FunctionFlagType.Both);
    public static final SqlLiteral flagLeading  =
            new SqlLiteral(FunctionFlagType.Leading);
    public static final SqlLiteral flagTrailing =
            new SqlLiteral(FunctionFlagType.Trailing);

    private static SqlFunctionTable instance;

    //~ Instance fields -------------------------------------------------------
    // REVIEW (jhyde): These fields are not constants (static final) so should
    // start with a lower-case letter.
    public final Set stringFuncNames = new LinkedHashSet();
    public final Set numericFuncNames = new LinkedHashSet();
    public final Set timeDateFuncNames = new LinkedHashSet();
    public final Set systemFuncNames = new LinkedHashSet();

    //~ SUBSTRING FUNCTIONS---------------
    /**
     * The character substring function:
     * <code>SUBSTRING(string FROM start [FOR length])</code>.
     *
     * <p>If the length parameter is a constant, the length
     * of the result is the minimum of the length of the input
     * and that length. Otherwise it is the length of the input.<p>
     */
    public final SqlFunction substringFunc =
        new SqlFunction("SUBSTRING", SqlOperatorTable.useFirstArgType, null,
                        null, SqlFunction.String){
            protected SaffronType inferType(SqlValidator validator,
                                            SqlValidator.Scope scope,
                                            SqlCall call) {
                //todo, overloading base class and then calling base class
                return super.inferType(validator, scope, call);
            }



            public String getAllowedSignatures() {
                StringBuffer ret=new StringBuffer();
                for (int i = 0; i < SqlOperatorTable.stringTypes.length; i++) {

                    if (i>0){
                        ret.append(NL);
                    }
                    ArrayList list = new ArrayList();
                    list.add(SqlOperatorTable.stringTypes[0]);
                    list.add(SqlTypeName.Integer);
                    ret.append(this.getSignature(list));
                    ret.append(NL);
                    list.add(SqlTypeName.Integer);
                    ret.append(this.getSignature(list));
                }
                return ret.toString();
            }

            protected void checkArgTypes(SqlCall call, SqlValidator validator,
                                         SqlValidator.Scope scope) {
                int n=call.operands.length;
                assert(3==n||2==n);
                SqlOperatorTable.typeNullableString.
                        checkThrows(validator,scope,call,call.operands[0],0);
                if (2==n) {
                    SqlOperatorTable.typeNullableNumeric.
                        checkThrows(validator,scope,call,call.operands[1],0);
                }
                else {
                    SaffronType t1=validator.deriveType(scope,call.operands[1]);
                    SaffronType t2=validator.deriveType(scope,call.operands[2]);

                    if (t1.isCharType()) {
                        SqlOperatorTable.typeNullableString.
                           checkThrows(validator,scope,call,call.operands[1],0);
                        SqlOperatorTable.typeNullableString.
                           checkThrows(validator,scope,call,call.operands[2],0);

                        isCharTypeComparableThrows(validator,scope,call.operands);

                       //todo length of escape must be 1, can check here or must do in calc?
                    } else {
                        SqlOperatorTable.typeNullableNumeric.
                           checkThrows(validator,scope,call,call.operands[1],0);
                        SqlOperatorTable.typeNullableNumeric.
                           checkThrows(validator,scope,call,call.operands[2],0);
                    }

                    if (!t1.isSameTypeFamily(t2)) {
                        throw call.newValidationSignatureError(validator,scope);
                    }
                }
            }

            public List getPossibleNumOfOperands() {
                List ret = new ArrayList(2);
                ret.add(new Integer(2));
                ret.add(new Integer(3));
                return ret;
            }

            public int getNumOfOperands(int disiredCount) {
                if (2==disiredCount){
                    return disiredCount;
                }
                return 3;
            }

            protected void checkNumberOfArg(SqlCall call) {
                int n=call.operands.length;
                if (n>3 || n < 2) {
                    throw Util.newInternal("todo: Wrong number of arguments to " + call);
                };
            }

            void unparse(
                    SqlWriter writer,
                    SqlNode[] operands,
                    int leftPrec,
                    int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,leftPrec,rightPrec);
                writer.print(" FROM ");
                operands[1].unparse(writer,leftPrec,rightPrec);

                if (3 == operands.length) {
                    writer.print(" FOR ");
                    operands[2].unparse(writer,leftPrec,rightPrec);
                }

                writer.print(")");
            }

        };


    //~ CONVERT ---------------
    public final SqlFunction convertFunc =
        new SqlFunction("CONVERT", null, null, null, SqlFunction.String) {

            void unparse(SqlWriter writer, SqlNode[] operands,
                         int leftPrec, int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,leftPrec,rightPrec);
                writer.print(" USING ");
                operands[1].unparse(writer,leftPrec,rightPrec);
                writer.print(")");
            }
        };

    //~ TRANSLATE ---------------
    public final SqlFunction translateFunc =
        new SqlFunction("TRANSLATE", null, null, null, SqlFunction.String) {

            void unparse(SqlWriter writer, SqlNode[] operands,
                         int leftPrec, int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,leftPrec,rightPrec);
                writer.print(" USING ");
                operands[1].unparse(writer,leftPrec,rightPrec);
                writer.print(")");
            }
        };

    //~ OVERLAY ---------------
    public final SqlFunction overlayFunc =
        new SqlFunction("OVERLAY", null, null, null, SqlFunction.String) {

            public SaffronType getType(SaffronTypeFactory typeFactory,
                                       SaffronType[] argTypes) {
                int n = argTypes.length;
                assert(3==n||4==n);


                SaffronType ret;
                if (argTypes[0].isCharType()) {
                    if (!isCharTypeComparable(argTypes, 0,1)) {
                        throw SaffronResource.instance().newValidationError(
                                argTypes[0].toString()+
                               " is not comparable to "+
                               argTypes[1].toString());
                    }


                    SqlCollation picked =
                            SqlCollation.getCoercibilityDyadicOperator(
                                    argTypes[0].getCollation(),
                                    argTypes[1].getCollation());
                    if (argTypes[0].getCollation().equals(picked)) {
                        //todo this is not correct.
                        //must return the right precision also
                        ret = argTypes[0];
                    } else if (argTypes[1].getCollation().equals(picked)) {
                        //todo this is not correct.
                        //must return the right precision also
                        ret = argTypes[1];
                    } else {
                        throw Util.newInternal("should never come here");
                    }
                } else {
                    //todo this is not correct.
                    //must return the right precision also
                    ret = argTypes[0];
                }

                return ret;
            }

            public SaffronType getType(SqlValidator validator,
                                       SqlValidator.Scope scope, SqlCall call) {
                checkArgTypes(call, validator, scope);

                int n = call.operands.length;
                assert(3==n||4==n);

                SaffronType[] argTypes = new SaffronType[n];

                for (int i = 0; i < n; i++) {
                    argTypes[i] = validator.deriveType(scope, call.operands[i]);
                }

                return getType(validator.typeFactory, argTypes);
            }

            public int getNumOfOperands(int disiredCount) {
                switch(disiredCount) {
                case 3: return 3;
                case 4: return 4;
                default: return 4;
                }
            }

            public List getPossibleNumOfOperands() {
                List ret = new ArrayList(2);
                ret.add(new Integer(3));
                ret.add(new Integer(4));
                return ret;
            }

            protected void checkArgTypes(SqlCall call, SqlValidator validator,
                                         SqlValidator.Scope scope) {
                switch(call.operands.length) {
                case 3:
                    SqlOperatorTable.typeNullableStringStringNotNullableInt.
                            check(validator, scope, call);
                    break;
                case 4:
                    SqlOperatorTable.typeNullableStringStringNotNullableIntInt.
                            check(validator, scope, call);
                    break;
                default: throw Util.needToImplement(this);
                }
            }

            void unparse(SqlWriter writer, SqlNode[] operands,
                         int leftPrec, int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,leftPrec,rightPrec);
                writer.print(" PLACING ");
                operands[1].unparse(writer,leftPrec,rightPrec);
                writer.print(" FROM ");
                operands[2].unparse(writer,leftPrec,rightPrec);
                if (4==operands.length) {
                    writer.print(" FOR ");
                    operands[3].unparse(writer,leftPrec,rightPrec);
                }
                writer.print(")");
            }
        };

    //~ TRIM ---------------
    public final SqlFunction trimFunc =
        new SqlFunction("TRIM", null, null,
                        SqlOperatorTable.typeNullableStringString,
                        SqlFunction.String) {

            public int getNumOfOperands(int disiredCount) {
                return 3;
            }

            public List getPossibleNumOfOperands() {
                List ret = new ArrayList(argTypeInference.getArgCount());
                ret.add(new Integer(getNumOfOperands(0)));
                return ret;
            }

            void unparse(SqlWriter writer, SqlNode[] operands,
                         int leftPrec, int rightPrec) {
                writer.print(name);
                writer.print("(");
                assert(operands[0] instanceof SqlLiteral);
                assert(((SqlLiteral)operands[0]).getValue() instanceof
                                                            FunctionFlagType);
                writer.print(((FunctionFlagType) ((SqlLiteral)operands[0]).
                                               getValue()).name_.toUpperCase());
                writer.print(" ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(" FROM ");
                operands[2].unparse(writer, leftPrec, rightPrec);
                writer.print(")");
            }

            public SqlCall createCall(SqlNode[] operands) {
                assert(3==operands.length);
                if (null==operands[0]) {
                    operands[0] = flagBoth;
                }

                if (null==operands[1]) {
                    operands[1] = SqlLiteral.StringLiteral.create("' '",null);
                }
                return super.createCall(operands);
            }

            protected void checkArgTypes(SqlCall call, SqlValidator validator,
                                         SqlValidator.Scope scope) {
                for(int i=1;i<3;i++){
                    if (!SqlOperatorTable.typeNullableString.check(
                            validator,scope,call.operands[i],0)) {
                        throw call.newValidationSignatureError(
                                validator, scope);
                    }
                }
            }


            public SaffronType getType(SaffronTypeFactory typeFactory,
                                       SaffronType[] argTypes) {
                assert(3==argTypes.length);
                return argTypes[2];
            }

            public SaffronType getType(SqlValidator validator,
                                       SqlValidator.Scope scope, SqlCall call) {
                checkArgTypes(call, validator, scope);

                SqlNode[] ops = new SqlNode[2];
                for (int i = 1; i < call.operands.length; i++) {
                    ops[i-1]=call.operands[i];
                }

                if (!isCharTypeComparable(validator, scope, ops)){
                    throw validator.newValidationError(
                                call.operands[1].toString()+
                                " is not comparable to "+
                                call.operands[2].toString());

                }

                return validator.deriveType(scope, call.operands[2]);
            }
        };


    //~ POSITION ---------------
    /**
     * Represents Position function
     */
    public final SqlFunction positionFunc =
            new SqlFunction("POSITION", SqlOperatorTable.useInteger, null,
                            SqlOperatorTable.typeNullableStringString,
                            SqlFunction.Numeric) {
                void unparse(SqlWriter writer,SqlNode[] operands,
                             int leftPrec,int rightPrec) {
                    writer.print(name);
                    writer.print("(");
                    operands[0].unparse(writer, leftPrec, rightPrec);
                    writer.print(" IN ");
                    operands[1].unparse(writer, leftPrec, rightPrec);
                    writer.print(")");
                }

                protected void checkArgTypes(SqlCall call,
                                             SqlValidator validator,
                                             SqlValidator.Scope scope) {
                    //check that the two operands are of same type.
                    SaffronType type0 =
                            validator.getValidatedNodeType(call.operands[0]);
                    SaffronType type1 =
                            validator.getValidatedNodeType(call.operands[1]);
                    if (!type0.isSameTypeFamily(type1) &&
                            !type1.isSameTypeFamily(type0)) {
                        throw call.newValidationSignatureError(
                                                    validator ,scope);
                    }

                    argTypeInference.check(validator,scope,call);
                }

            };

    //~ CHAR LENGTH ---------------
    public final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH", SqlOperatorTable.useInteger, null,
                        SqlOperatorTable.typeNullableVarchar,
                        SqlFunction.Numeric);

    public final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH", SqlOperatorTable.useInteger, null,
                        SqlOperatorTable.typeNullableVarchar,
                        SqlFunction.Numeric);

    //~ UPPER ---------------
    public final SqlFunction upperFunc =
        new SqlFunction("UPPER", SqlOperatorTable.useFirstArgType, null,
                        SqlOperatorTable.typeNullableVarchar,
                        SqlFunction.String);

    //~ LOWER ---------------
    public final SqlFunction lowerFunc =
        new SqlFunction("LOWER", SqlOperatorTable.useFirstArgType, null,
                        SqlOperatorTable.typeNullableVarchar,
                        SqlFunction.String);

    //~ INIT CAP ---------------
    public final SqlFunction initcapFunc =
        new SqlFunction("INITCAP", SqlOperatorTable.useFirstArgType, null,
                        SqlOperatorTable.typeNullableVarchar,
                        SqlFunction.String);


    //~ POWER ---------------
    public final SqlFunction powFunc =
        new SqlFunction("POW", SqlOperatorTable.useBiggest, null,
                        SqlOperatorTable.typeNumericNumeric,
                        SqlFunction.Numeric);

    //~ MOD ---------------
    public final SqlFunction modFunc =
        new SqlFunction("MOD", SqlOperatorTable.useBiggest, null,
                        SqlOperatorTable.typeNumericNumeric,
                        SqlFunction.Numeric);

    //~ LOGARITHMS ---------------
    public final SqlFunction lnFunc =
        new SqlFunction("LN", SqlOperatorTable.useDouble, null,
                        SqlOperatorTable.typeNumeric, SqlFunction.Numeric);

    public final SqlFunction logFunc =
        new SqlFunction("LOG", SqlOperatorTable.useDouble, null,
                        SqlOperatorTable.typeNumeric, SqlFunction.Numeric);

    //~ ABS VALUE ---------------
    public final SqlFunction absFunc =
        new SqlFunction("ABS", SqlOperatorTable.useBiggest, null,
                        SqlOperatorTable.typeNumeric, SqlFunction.Numeric);


    //~ NULLIF - Conditional Expression ---------------
    public final SqlFunction nullIfFunc =
        new SqlFunction("NULLIF", null, null, null, SqlFunction.System){
            public SqlCall createCall(SqlNode[] operands) {
                SqlNodeList whenList = new SqlNodeList();
                SqlNodeList thenList = new SqlNodeList();
                whenList.add(operands[1]);
                thenList.add(SqlLiteral.createNull());
                return SqlOperatorTable.instance().caseOperator.createCall(
                        operands[0],whenList,thenList,operands[0]);
            }

            public int getNumOfOperands(int disiredCount) {
                return 2;
            }
        };

    //~ COALESCE - Conditional Expression ---------------
    public final SqlFunction coalesceFunc =
    new SqlFunction("COALESCE", null, null, null, SqlFunction.System){
        public SqlCall createCall(SqlNode[] operands) {
            Util.pre(operands.length>=2,"operands.length>=2");
            return createCall(operands, 0);
        }

        private SqlCall createCall(SqlNode[] operands, int start) {
            SqlNodeList whenList = new SqlNodeList();
            SqlNodeList thenList = new SqlNodeList();
            whenList.add(
                    SqlOperatorTable.instance().isNotNullOperator.createCall(
                                            operands[start]));
            thenList.add(operands[start]);
            if (2==(operands.length-start)){
                return SqlOperatorTable.instance().caseOperator.createCall(
                        null,whenList,thenList,operands[start+1]);
            }
            return SqlOperatorTable.instance().caseOperator.createCall(
                    null,whenList,thenList,this.createCall(operands, start+1));
        }

        public int getNumOfOperands(int disiredCount) {
            return 2;
        }
    };

    public final SqlFunction localTimeFunc =
               new SqlFunction("LOCALTIME", null, null,
                       SqlOperatorTable.typePositiveIntegerLiteral,SqlFunction.TimeDate) {
                   protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                       int precision = ((java.math.BigInteger)((SqlLiteral)call.operands[0]).getValue()).intValue();
                       if (precision < 0) {
                           throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                       }
                       return validator.typeFactory.createSqlType(SqlTypeName.Time, precision);
                   }

                   public SaffronType getType(SaffronTypeFactory typeFactory,
                                              SaffronType[] argTypes) {
                    //   assert false;
                       int precision = 0; // todo: access the value of the literal parmaeter
                       return typeFactory.createSqlType(SqlTypeName.Time, precision);
                   }

                   protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {

                       super.checkArgTypes(call, validator, scope);    //To change body of overridden methods use File | Settings | File Templates.
                   }

                   protected void checkNumberOfArg(SqlCall call) {
                       if (call.getOperands().length > 1) {
                           throw Util.newInternal("todo: function takes 0 or 1 parameters");
                       }
                   }
               };
    public final SqlFunction localTimestampFunc =
                new SqlFunction("LOCALTIMESTAMP", null, null,
                        SqlOperatorTable.typePositiveIntegerLiteral,SqlFunction.TimeDate) {
                    protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                        int precision = ((java.math.BigInteger)((SqlLiteral)call.operands[0]).getValue()).intValue();
                        if (precision < 0) {
                            throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                        }
                        return validator.typeFactory.createSqlType(SqlTypeName.Timestamp, precision);
                    }

                    public SaffronType getType(SaffronTypeFactory typeFactory,
                                               SaffronType[] argTypes) {
                     //   assert false;
                        int precision = 0; // todo: access the value of the literal parmaeter
                        return typeFactory.createSqlType(SqlTypeName.Time, precision);
                    }

                    protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {

                        super.checkArgTypes(call, validator, scope);    //To change body of overridden methods use File | Settings | File Templates.
                    }

                    protected void checkNumberOfArg(SqlCall call) {
                        if (call.getOperands().length > 1) {
                            throw Util.newInternal("todo: function takes 0 or 1 parameters");
                        }
                    }
                };


    public final SqlFunction currentTimeFunc =
               new SqlFunction("CURRENT_TIME", null, null,
                       SqlOperatorTable.typePositiveIntegerLiteral,SqlFunction.TimeDate) {
                   protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                       int precision = ((java.math.BigInteger)((SqlLiteral)call.operands[0]).getValue()).intValue();
                       if (precision < 0) {
                           throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                       }
                       return validator.typeFactory.createSqlType(SqlTypeName.Time, precision);
                   }

                   public SaffronType getType(SaffronTypeFactory typeFactory,
                                              SaffronType[] argTypes) {
                    //   assert false;
                       int precision = 0; // todo: access the value of the literal parmaeter
                       return typeFactory.createSqlType(SqlTypeName.Time, precision);
                   }

                   protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {

                       super.checkArgTypes(call, validator, scope);    //To change body of overridden methods use File | Settings | File Templates.
                   }

                   protected void checkNumberOfArg(SqlCall call) {
                       if (call.getOperands().length > 1) {
                           throw Util.newInternal("todo: function takes 0 or 1 parameters");
                       }
                   }
               };
       public final SqlFunction currentTimestampFunc =
               new SqlFunction("CURRENT_TIMESTAMP", null, null,
                       SqlOperatorTable.typePositiveIntegerLiteral,SqlFunction.TimeDate) {
                   protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                       int precision = ((java.math.BigInteger)((SqlLiteral)call.operands[0]).getValue()).intValue();
                       if (precision < 0) {
                           throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                       }
                       return validator.typeFactory.createSqlType(SqlTypeName.Timestamp, precision);
                   }

                   public SaffronType getType(SaffronTypeFactory typeFactory,
                                              SaffronType[] argTypes) {
                    //   assert false;
                       int precision = 0; // todo: access the value of the literal parmaeter
                       return typeFactory.createSqlType(SqlTypeName.Timestamp, precision);
                   }

                   protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {

                       super.checkArgTypes(call, validator, scope);    //To change body of overridden methods use File | Settings | File Templates.
                   }

                   protected void checkNumberOfArg(SqlCall call) {
                       if (call.getOperands().length > 1) {
                           throw Util.newInternal("todo: function takes 0 or 1 parameters");
                       }
                   }
               };
    public final SqlFunction currentDateFunc =
            new SqlFunction("CURRENT_DATE", null, null,
                    null, SqlFunction.TimeDate) {
                protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                    return validator.typeFactory.createSqlType(SqlTypeName.Date);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    //   assert false;
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
                    if (call.operands.length != 0) {
                         throw call.newValidationSignatureError(validator, scope);
                    }
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 0) {
                        throw Util.newInternal("todo: function takes no parameters");
                    }
                }
                // no argTypeInference, so must override these methods.  Probably need a niladic version of that.

                public List getPossibleNumOfOperands() {
                    List ret = new ArrayList(1);
                    ret.add(new Integer(0));
                    return ret;
                }
            };


    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p/>The target type is simply stored as
     * the return type, not an explicit operand. For example, the expression
     * <code>CAST(1 + 2 AS DOUBLE)</code> will become a call to
     * <code>CAST</code> with the expression <code>1 + 2</code> as its only
     * operand.
     */
    public final SqlFunction cast = new SqlFunction(
        "CAST",null,SqlOperatorTable.useReturnForParam, null,
         SqlFunction.System) {

        public int getNumOfOperands(int disiredCount) {
            return 1;
        }
    };

    /**
     * Multi-map from function name to a list of functions with that name.
     */
    // todo: when we support function overloading, we will use MultiMap
    // REVIEW (jhyde): Regardless of what the above comment says, we need to
    //   switch to net.sf.saffron.util.MultiMap now. We're already storing
    //   lists in this thing!
    private final HashMap mapNameToFunc = new HashMap();

    //~ Constructors ----------------------------------------------------------

    protected SqlFunctionTable() {
        // Use reflection to register the expressions stored in public fields.
        Field[] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (SqlFunction.class.isAssignableFrom(field.getType())) {
                try {
                    SqlFunction op = (SqlFunction) field.get(this);
                    register(op);
                } catch (IllegalArgumentException e) {
                    throw Util.newInternal(
                            e,
                            "Error while initializing operator table");
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(
                            e,
                            "Error while initializing operator table");
                }
            }
        }
    }


    //~ Methods ---------------------------------------------------------------
    public static SqlCall createCall(String funName, SqlNode[] operands) {
        List funs = instance().lookup(funName);
        if ((null!=funs)&&(funs.size()>0)){
            return ((SqlFunction) funs.get(0)).createCall(operands);
        }

        return new SqlFunction(funName, null, null,null).createCall(operands);
     }

    /**
     * Retrieves the singleton, creating it if necessary.
     */
    public static SqlFunctionTable instance() {
        if (instance == null) {
            instance = new SqlFunctionTable();
        }
        return instance;
    }


    /**
     * Retrieves a list of overloading function by a given name.
     * @return If no function exists, null is returned,
     *         else retrieves a list of overloading function by a given name.
     */
    public List lookup(String funcName) {
        return (List) mapNameToFunc.get(funcName);
    }

    /**
     * Register function to the table.
     * @param function
     */
    public void register(SqlFunction function) {

        List functionList;

        if (mapNameToFunc.get(function.name) != null) {
            functionList = (List) mapNameToFunc.get(function.name);
        } else {
            functionList = new LinkedList();
            mapNameToFunc.put(function.name, functionList);
            SqlFunction.SqlFuncTypeName funcType = function.getFunctionType();
            assert (funcType != null) :
                    "Function type for "+function.name+" not set";
            switch (funcType.getOrdinal()) {
            case SqlFunction.String_ordinal:
                stringFuncNames.add(function.name);
                break;
            case SqlFunction.Numeric_ordinal:
                numericFuncNames.add(function.name);
                break;
            case SqlFunction.TimeDate_ordinal:
                timeDateFuncNames.add(function.name);
                break;
            case SqlFunction.System_ordinal:
                systemFuncNames.add(function.name);
                break;
            }

        }
        functionList.add(function);
    }

    private SqlFunction[] lookup(String name, int numberOfParams) {
        List funcList = (List) mapNameToFunc.get(name);
        if (null == funcList) {
            return null;
        }

        List candidateList = new LinkedList();
        for (int i = 0; i < funcList.size(); i++) {
            SqlFunction function = (SqlFunction) funcList.get(i);
            List possibleNums = function.getPossibleNumOfOperands();
            if (possibleNums.contains(new Integer(numberOfParams))) {
                candidateList.add(function);
            }
        }
        return (SqlFunction[]) candidateList.toArray(


                new SqlFunction[candidateList.size()]);

    }

    /**
     * Chose the best fit function
     * @param funcName
     * @param argTypes
     * @return
     */
    public SqlFunction lookup(String funcName, SaffronType[] argTypes) {
        // The number of defined parameters need to match the invocation
        SqlFunction[] functions = lookup(funcName, argTypes.length);
        if ((null == functions) || (0==functions.length)) {
            return null;
        } else if (functions.length == 1) {
            return functions[0];
        }

        ArrayList candidates = new ArrayList();
        for (int i = 0; i < functions.length; i++) {
            SqlFunction function = functions[i];
            if (function.isMatchParamType(argTypes)) {
                candidates.add(function);
            }
        }

        if (candidates.size() == 0) {
            return null;
        } else if (candidates.size() == 1) {
            return (SqlFunction) candidates.get(1);
        }

        // Next, consider each argument of the function invocation, from left to right. For each argument,
        // eliminate all functions that are not the best match for that argument. The best match for a given
        // argument is the first data type appearing in the precedence list corresponding to the argument data
        // type in Table 3 for which there exists a function with a parameter of that data type. Lengths,
        // precisions, scales and the "FOR BIT DATA" attribute are not considered in this comparison.
        // For example, a DECIMAL(9,1) argument is considered an exact match for a DECIMAL(6,5) parameter,
        // and a VARCHAR(19) argument is an exact match for a VARCHAR(6) parameter.
        // Reference: http://www.pdc.kth.se/doc/SP/manuals/db2-5.0/html/db2s0/db2s067.htm#HDRUDFSEL
        //
        for (int i = 0; i < argTypes.length; i++) {
            SaffronType argType = argTypes[i];
            throw Util.needToImplement("Function resolution with different types is not implemented yet.");
        }


        return null;
    }


}


// End SqlFunctionTable.java
