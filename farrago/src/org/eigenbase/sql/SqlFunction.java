/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.Util;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 */
public class SqlFunction extends SqlOperator
{
    //~ Static fields/initializers --------------------------------------------

    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlFuncTypeName [] {
                SqlFuncTypeName.String, SqlFuncTypeName.Numeric,
                SqlFuncTypeName.TimeDate, SqlFuncTypeName.System,
            });

    //~ Instance fields -------------------------------------------------------

    private final SqlFuncTypeName functionType;

    private final SqlIdentifier sqlIdentifier;

    private final RelDataType [] paramTypes;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name of builtin function
     *
     * @param kind kind of operator implemented by function
     *
     * @param typeInference strategy to use for return type inference
     *
     * @param paramTypeInference strategy to use for parameter type inference
     *
     * @param paramTypes strategy to use for parameter type checking
     *
     * @param funcType categorization for function
     */
    public SqlFunction(
        String name,
        SqlKind kind,
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking paramTypes,
        SqlFuncTypeName funcType)
    {
        super(name, kind, 100, 100, typeInference, paramTypeInference,
            paramTypes);
        this.functionType = funcType;

        // NOTE jvs 18-Jan-2005:  we leave sqlIdentifier as null to indicate
        // that this is a builtin.  Same for paramTypes.
        this.sqlIdentifier = null;
        this.paramTypes = null;
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a
     * possibly qualified name.  This name must be resolved into either a
     * builtin function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     *
     * @param typeInference strategy to use for return type inference
     *
     * @param paramTypeInference strategy to use for parameter type inference
     *
     * @param paramTypeChecking strategy to use for parameter type checking
     *
     * @param paramTypes array of parameter types
     *
     * @param funcType function category
     */
    public SqlFunction(
        SqlIdentifier sqlIdentifier, 
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking paramTypeChecking,
        RelDataType [] paramTypes, 
        SqlFuncTypeName funcType)
    {
        super(
            sqlIdentifier.names[sqlIdentifier.names.length - 1],
            SqlKind.Function,
            100,
            100, 
            typeInference,
            paramTypeInference,
            paramTypeChecking);
        this.sqlIdentifier = sqlIdentifier;
        this.functionType = funcType;
        this.paramTypes = paramTypes;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Function;
    }

    /**
     * @return fully qualified name of function, or null for a builtin
     * function
     */
    public SqlIdentifier getSqlIdentifier()
    {
        return sqlIdentifier;
    }

    /**
     * @return fully qualified name of function
     */
    public SqlIdentifier getNameAsId()
    {
        if (sqlIdentifier != null) {
            return sqlIdentifier;
        }
        return new SqlIdentifier(name, null);
    }

    /**
     * @return array of parameter types, or null for builtin function
     */
    public RelDataType [] getParamTypes()
    {
        return paramTypes;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        getSyntax().unparse(writer, this, operands, leftPrec, rightPrec);
    }

    /**
     *
     * @return function type {@link SqlFuncTypeName}
     */
    public SqlFuncTypeName getFunctionType()
    {
        return this.functionType;
    }

    /**
     * Looks up a kind from its ordinal.
     */
    public static SqlFuncTypeName get(int ordinal)
    {
        return (SqlFuncTypeName) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a kind from its name.
     */
    public static SqlFuncTypeName get(String name)
    {
        return (SqlFuncTypeName) enumeration.getValue(name);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Enumeration of the types supported functions.
     */
    public static class SqlFuncTypeName extends EnumeratedValues.BasicValue
    {
        public static final int String_ordinal = 0;

        /** String function type **/
        public static final SqlFuncTypeName String =
            new SqlFuncTypeName("STRING", String_ordinal, "String function");
        
        public static final int Numeric_ordinal = 1;

        /** Numeric function type **/
        public static final SqlFuncTypeName Numeric =
            new SqlFuncTypeName("NUMERIC", Numeric_ordinal, "Numeric function");
        
        public static final int TimeDate_ordinal = 2;

        /** Time and date function type **/
        public static final SqlFuncTypeName TimeDate =
            new SqlFuncTypeName("TIMEDATE", TimeDate_ordinal,
                "Time and date function");
        
        public static final int System_ordinal = 3;

        /** System function type **/
        public static final SqlFuncTypeName System =
            new SqlFuncTypeName("SYSTEM", System_ordinal, "System function");

        public static final int UserDefinedFunction_ordinal = 4;
        
        /** User-defined function type **/
        public static final SqlFuncTypeName UserDefinedFunction =
            new SqlFuncTypeName(
                "UDF", UserDefinedFunction_ordinal, "User-defined function");

        public static final int UserDefinedProcedure_ordinal = 5;
        
        /** User-defined procedure type **/
        public static final SqlFuncTypeName UserDefinedProcedure =
            new SqlFuncTypeName(
                "UDP", UserDefinedProcedure_ordinal, "User-defined procedure");

        public static final int UserDefinedConstructor_ordinal = 6;
        
        /** User-defined constructor type **/
        public static final SqlFuncTypeName UserDefinedConstructor =
            new SqlFuncTypeName(
                "UDC", UserDefinedConstructor_ordinal,
                "User-defined constructor");

        private SqlFuncTypeName(
            String name,
            int ordinal,
            String description)
        {
            super(name, ordinal, description);
        }
    }
}


// End SqlFunction.java
