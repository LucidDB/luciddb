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
import net.sf.saffron.core.SaffronType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Vector;
import java.util.List;
import java.util.LinkedList;

/**
 * A <code>SqlFunctionTable</code> is a singleton which contains an instance of
 * each SQL built-in function.
 *
 * @author kinkoi
 * @since Jan 05, 2004
 * @version $Id$
 **/


public class SqlFunctionTable {
    //~ Static fields/initializers --------------------------------------------

    private static SqlFunctionTable instance;

    //~ Instance fields -------------------------------------------------------
    public final SqlFunction substrFunc =
            new SqlFunction("SUBSTR", SqlOperatorTable.useFirstArgType, null, SqlOperatorTable.typeVarcharIntInt) {
                protected SaffronType inferType(SqlValidator validator,
                                                SqlValidator.Scope scope, SqlCall call) {
                    //todo, overloading base class and then calling base class
                    return super.inferType(validator, scope, call);
                }
            };


    // The length parameter is a constant, the length
    // of the result is the minimum of the length of the input
    // and that length.
    public final SqlFunction substr2Func =
            new SqlFunction("SUBSTR", SqlOperatorTable.useFirstArgType, null, SqlOperatorTable.typeVarcharInt) {
                protected SaffronType inferType(SqlValidator validator,
                                                SqlValidator.Scope scope, SqlCall call) {
                    //todo, overloading base class and then calling base class
                    return super.inferType(validator, scope, call);
                }
            };

    public final SqlFunction upperFunc =
            new SqlFunction("UPPER", SqlOperatorTable.useFirstArgType, null, SqlOperatorTable.typeNullableVarchar);

    public final SqlFunction lowerFunc =
            new SqlFunction("LOWER", SqlOperatorTable.useFirstArgType, null, SqlOperatorTable.typeNullableVarchar);

    public final SqlFunction initcapFunc =
            new SqlFunction("INITCAP", SqlOperatorTable.useFirstArgType, null, SqlOperatorTable.typeNullableVarchar);

    public final SqlFunction powFunc =
            new SqlFunction("POW", SqlOperatorTable.useBiggest, null, SqlOperatorTable.typeNumericNumeric);

    public final SqlFunction modFunc =
            new SqlFunction("MOD", SqlOperatorTable.useBiggest, null, SqlOperatorTable.typeNumericNumeric);

    public final SqlFunction lnFunc =
            new SqlFunction("LN", SqlOperatorTable.doubleType, null, SqlOperatorTable.typeNumeric);

    public final SqlFunction logFunc =
            new SqlFunction("LOG", SqlOperatorTable.doubleType, null, SqlOperatorTable.typeNumeric);

    public final SqlFunction abcFunc =
            new SqlFunction("ABS", SqlOperatorTable.useBiggest, null, SqlOperatorTable.typeNumeric);


    public final SqlFunction nullIfFunc =
            new SqlFunction("NULLIF", null, null, null){
                public SqlCall createCall(SqlNode[] operands) {
                    SqlNodeList whenList = new SqlNodeList();
                    SqlNodeList thenList = new SqlNodeList();
                    whenList.add(operands[1]);
                    thenList.add(SqlLiteral.createNull());
                    return SqlOperatorTable.instance().caseOperator.createCall(operands[0],whenList,thenList,operands[0]);
                }

                public int getNumOfOperands() {
                    return 2;
                }
            };

    public final SqlFunction coalesceFunc =
            new SqlFunction("COALESCE", null, null, null){
                public SqlCall createCall(SqlNode[] operands) {
                    Util.pre(operands.length>=2,"operands.length>=2");
                    return createCall(operands, 0);
                }

                private SqlCall createCall(SqlNode[] operands, int start) {
                    SqlNodeList whenList = new SqlNodeList();
                    SqlNodeList thenList = new SqlNodeList();
                    whenList.add(SqlOperatorTable.instance().isNotNullOperator.createCall(operands[start]));
                    thenList.add(operands[start]);
                    if (2==(operands.length-start)){
                        return SqlOperatorTable.instance().caseOperator.createCall(null,whenList,thenList,operands[start+1]);
                    }
                    return SqlOperatorTable.instance().caseOperator.createCall(null,whenList,thenList,this.createCall(operands, start+1));
                }

                public int getNumOfOperands() {
                    return 2;
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
        "CAST",null,SqlOperatorTable.useReturnForParam, null) {

        public int getNumOfOperands() {
            return 1;
        }
    };


    // todo: when we support function overloading, we will use MultiMap
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
     * @return If no function exists, null is returned, else retrieves a list of overloading function by a given name.
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
            if (function.getNumOfOperands() == numberOfParams) {
                candidateList.add(function);
            }
        }
        return (SqlFunction[]) candidateList.toArray(new SqlFunction[candidateList.size()]);

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


        Vector candidates = new Vector();
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