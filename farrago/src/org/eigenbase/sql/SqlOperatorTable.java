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
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.util.MultiMap;
import org.eigenbase.util.Util;

import java.lang.reflect.Field;
import java.util.*;


/**
 * <code>SqlOperatorTable</code> is a (almost) a singleton. Almost since it
 * get instance method returns an instance of {@link SqlStdOperatorTable} which
 * contain all defined operators and functions.
 */
public abstract class SqlOperatorTable
{
    //~ Instance fields -------------------------------------------------------

    private final MultiMap operators = new MultiMap();
    private final HashMap mapNameToOp = new HashMap();
    public final Set stringFuncNames = new LinkedHashSet();
    public final Set numericFuncNames = new LinkedHashSet();
    public final Set timeDateFuncNames = new LinkedHashSet();
    public final Set systemFuncNames = new LinkedHashSet();

    /**
     * Multi-map from function name to a list of functions with that name.
     */
    private final MultiMap mapNameToFunc = new MultiMap();

    //~ Constructors ----------------------------------------------------------

    protected SqlOperatorTable()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Call this method after constructing an operator table. It can't be
     * part of the constructor, because the sub-class' constructor needs to
     * complete first.
     */
    public final void init()
    {
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Field field = fields[i];
                if (SqlFunction.class.isAssignableFrom(field.getType())) {
                    SqlFunction op = (SqlFunction) field.get(this);
                    if (op != null) {
                        register(op);
                    }
                } else if (SqlOperator.class.isAssignableFrom(field.getType())) {
                    SqlOperator op = (SqlOperator) field.get(this);
                    register(op);

                }
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            }
        }
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance, creating it if necessary.
     */
    public static SqlOperatorTable instance()
    {
        return SqlStdOperatorTable.std();
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance of the table of standard operators.
     */
    public static SqlStdOperatorTable std()
    {
        return SqlStdOperatorTable.std();
    }

    /**
     * Retrieves an operator by its name and syntactic type.
     */
    public SqlOperator lookup(
        String opName,
        SqlSyntax syntax)
    {
        final List list = operators.getMulti(opName.toUpperCase());
        for (int i = 0, n = list.size(); i < n; i++) {
            SqlOperator op = (SqlOperator) list.get(i);
            if (op.getSyntax() == syntax) {
                return op;
            }
        }
        switch (syntax.ordinal) {
        case SqlSyntax.Binary_ordinal:
            return (SqlBinaryOperator) mapNameToOp.get(opName + ":BINARY");
        case SqlSyntax.Prefix_ordinal:
            return (SqlPrefixOperator) mapNameToOp.get(opName + ":PREFIX");
        case SqlSyntax.Postfix_ordinal:
            return (SqlPostfixOperator) mapNameToOp.get(opName + ":POSTFIX");
        case SqlSyntax.Function_ordinal:
            throw Util.newInternal("Use lookupFunction to lookup function");
        default:
            throw syntax.unexpected();
        }
    }

    public void register(SqlOperator op)
    {
        operators.putMulti(op.name, op);
        if (op instanceof SqlBinaryOperator) {
            mapNameToOp.put(op.name + ":BINARY", op);
        } else if (op instanceof SqlPrefixOperator) {
            mapNameToOp.put(op.name + ":PREFIX", op);
        } else if (op instanceof SqlPostfixOperator) {
            mapNameToOp.put(op.name + ":POSTFIX", op);
        }
    }

    public SqlCall createCall(
        String funName,
        SqlNode [] operands,
        ParserPosition pos)
    {
        List funs = lookupFunctionsByName(funName);
        final SqlFunction fun;
        if (!funs.isEmpty()){
            assert funs.size() == 1 : "operator overloading not implemented";
            fun = (SqlFunction) funs.get(0);
        } else {
            // create an ad hoc function for just this call
            // REVIEW/TODO wael: why is this call neccessary? I tried removing
            // it and tests failed.
            fun = new SqlFunction(funName, SqlKind.Function, null, null, null,
                null);
        }
        return fun.createCall(operands, pos);
    }

    /**
     * Retrieves a list of overloading function by a given name.
     * @return If no function exists, null is returned,
     *         else retrieves a list of overloading function by a given name.
     */
    public List lookupFunctionsByName(String funcName)
    {
        return mapNameToFunc.getMulti(funcName);
    }

    /**
     * Register function to the table.
     * @param function
     */
    public void register(SqlFunction function)
    {
        mapNameToFunc.putMulti(function.name, function);
        SqlFunction.SqlFuncTypeName funcType = function.getFunctionType();
        assert (funcType != null) : "Function type for " + function.name
        + " not set";
        switch (funcType.getOrdinal()) {
        case SqlFunction.SqlFuncTypeName.String_ordinal:
            stringFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.Numeric_ordinal:
            numericFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.TimeDate_ordinal:
            timeDateFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.System_ordinal:
            systemFuncNames.add(function.name);
            break;
        default: Util.needToImplement(funcType);
        }
    }

    private SqlFunction [] lookupFunctionsByNameAndArgCount(
        String name,
        int numberOfParams)
    {
        List funcList = mapNameToFunc.getMulti(name);
        if (funcList.isEmpty()) {
            return null;
        }

        List candidateList = new LinkedList();
        for (int i = 0; i < funcList.size(); i++) {
            SqlFunction function = (SqlFunction) funcList.get(i);
            SqlOperator.OperandsCountDescriptor od =
                function.getOperandsCountDescriptor();
            if (od.isVariadic()
                    || od.getPossibleNumOfOperands().contains(
                        new Integer(numberOfParams))) {
                candidateList.add(function);
            }
        }
        return (SqlFunction []) candidateList.toArray(
            new SqlFunction[candidateList.size()]);
    }

    /**
     * Chose the best fit function
     * @param funcName
     * @param argTypes
     * @return
     */
    public SqlFunction lookupFunction(
        String funcName,
        RelDataType [] argTypes)
    {
        // The number of defined parameters need to match the invocation
        SqlFunction [] functions =
            lookupFunctionsByNameAndArgCount(funcName, argTypes.length);
        if ((null == functions) || (0 == functions.length)) {
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

        // Next, consider each argument of the function invocation, from left
        // to right. For each argument, eliminate all functions that are not
        // the best match for that argument. The best match for a given
        // argument is the first data type appearing in the precedence list
        // corresponding to the argument data type in Table 3 for which there
        // exists a function with a parameter of that data type. Lengths,
        // precisions, scales and the "FOR BIT DATA" attribute are not
        // considered in this comparison.  For example, a DECIMAL(9,1) argument
        // is considered an exact match for a DECIMAL(6,5) parameter, and a
        // VARCHAR(19) argument is an exact match for a VARCHAR(6) parameter.
        // Reference:
        // http://www.pdc.kth.se/doc/SP/manuals/db2-5.0/html/db2s0/db2s067.htm#HDRUDFSEL
        //
        for (int i = 0; i < argTypes.length; i++) {
            throw Util.needToImplement("Function resolution with different "
                + "types is not implemented yet.");
        }
        return null;
    }

    /**
     * Returns a list of all functions and operators in this table.
     * Used for automated testing.
     */
    public ArrayList getOperatorList()
    {
        ArrayList list = new ArrayList();

        Iterator it = operators.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlOperator operator = (SqlOperator) mapEntry.getValue();
            list.add(operator);
        }

        it = mapNameToFunc.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlFunction function = (SqlFunction) mapEntry.getValue();
            list.add(function);
        }

        return list;
    }

}


// End SqlOperatorTable.java

