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

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.Util;

/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 */
public abstract class SqlFunction extends SqlOperator
{
    //~ Instance fields -------------------------------------------------------
    private SqlFuncTypeName functionType=null;


    //~ Constructors ----------------------------------------------------------

    public SqlFunction(
        String name, TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference paramTypes)
    {
        super(name,SqlKind.Function,100,100,typeInference,paramTypeInference,
                paramTypes);
    }

    public SqlFunction(
        String name, SqlKind kind, TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference paramTypes, SqlFuncTypeName funcType)
    {
        super(name, kind, 100, 100, typeInference, paramTypeInference,
                paramTypes);
        this.functionType = funcType;
    }

    public SqlFunction(
        String name, SqlKind kind, TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference paramTypes)
    {
        super(name,kind,100,100,typeInference,paramTypeInference,
                paramTypes);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Function;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        unparseFunctionSyntax(this,writer,operands);
    }

    public static void unparseFunctionSyntax(
        SqlOperator operator,
        SqlWriter writer,
        SqlNode [] operands)
    {
        writer.print(operator.name);
        writer.print('(');
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            if (i > 0) {
                writer.print(", ");
            }
            operand.unparse(writer,0,0);
        }
        writer.print(')');
    }

    /**
     *
     * @return function type {@link SqlFuncTypeName}
     */
    public SqlFuncTypeName getFunctionType()
    {
        return this.functionType;
    }

    public boolean isMatchParamType(SaffronType[] paramTypes) {
        throw Util.needToImplement("Need to implement isMatchParamType method.");
    }

    /**
     * Enumeration of the types supported functions.
     */
    public static class SqlFuncTypeName extends EnumeratedValues.BasicValue {
        private SqlFuncTypeName(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final int String_ordinal = 0;
        /** String function type **/
        public static final SqlFuncTypeName String = new SqlFuncTypeName("STRING", String_ordinal, "String function");

        public static final int Numeric_ordinal = 1;
        /** Numeric function type **/
        public static final SqlFuncTypeName Numeric = new SqlFuncTypeName("NUMERIC", Numeric_ordinal, "Numeric function");

        public static final int TimeDate_ordinal = 2;
        /** Time and date function type **/
        public static final SqlFuncTypeName TimeDate = new SqlFuncTypeName("TIMEDATE", TimeDate_ordinal, "Time and date function");

        public static final int System_ordinal = 3;
        /** System function type **/
        public static final SqlFuncTypeName System = new SqlFuncTypeName("SYSTEM", System_ordinal, "System function");
    }

    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new SqlFuncTypeName[] {
                SqlFuncTypeName.String,
                SqlFuncTypeName.Numeric,
                SqlFuncTypeName.TimeDate,
                SqlFuncTypeName.System,
            });

    /**
     * Looks up a kind from its ordinal.
     */
    public static SqlFuncTypeName get(int ordinal) {
        return (SqlFuncTypeName) enumeration.getValue(ordinal);
    }
    /**
     * Looks up a kind from its name.
     */
    public static SqlFuncTypeName get(String name) {
        return (SqlFuncTypeName) enumeration.getValue(name);
    }

}


// End SqlFunction.java
