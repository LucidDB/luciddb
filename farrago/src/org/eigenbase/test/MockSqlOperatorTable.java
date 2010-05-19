/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
// Portions Copyright (C) 2006 John V. Sichi
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
package org.eigenbase.test;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator
 * table, plus a list of operators.
 *
 * @author jhyde
 * @version $Id$
 */
public class MockSqlOperatorTable
    extends ChainedSqlOperatorTable
{
    //~ Instance fields --------------------------------------------------------

    private final ListSqlOperatorTable listOpTab = new ListSqlOperatorTable();

    //~ Constructors -----------------------------------------------------------

    public MockSqlOperatorTable(SqlOperatorTable parentTable)
    {
        add(parentTable);
        add(listOpTab);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds an operator to this table.
     */
    public void addOperator(SqlOperator op)
    {
        listOpTab.add(op);
    }

    public static void addRamp(MockSqlOperatorTable opTab)
    {
        opTab.addOperator(
            new SqlFunction(
                "RAMP",
                SqlKind.Function,
                null,
                null,
                SqlTypeStrategies.otcNumeric,
                SqlFunctionCategory.UserDefinedFunction)
            {
                public RelDataType inferReturnType(
                    SqlOperatorBinding opBinding)
                {
                    final RelDataTypeFactory typeFactory =
                        opBinding.getTypeFactory();
                    final RelDataType [] types =
                    { typeFactory.createSqlType(SqlTypeName.INTEGER) };
                    final String [] fieldNames = new String[] { "I" };
                    return typeFactory.createStructType(types, fieldNames);
                }
            });

        opTab.addOperator(
            new SqlFunction(
                "DEDUP",
                SqlKind.Function,
                null,
                null,
                SqlTypeStrategies.otcVariadic,
                SqlFunctionCategory.UserDefinedFunction)
            {
                public RelDataType inferReturnType(
                    SqlOperatorBinding opBinding)
                {
                    final RelDataTypeFactory typeFactory =
                        opBinding.getTypeFactory();
                    final RelDataType [] types =
                    { typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024) };
                    final String [] fieldNames = new String[] { "NAME" };
                    return typeFactory.createStructType(types, fieldNames);
                }
            });
    }
}

// End MockSqlOperatorTable.java
