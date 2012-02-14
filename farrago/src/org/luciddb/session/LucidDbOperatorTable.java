/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.luciddb.session;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * LucidDbOperatorTable extends {@link SqlStdOperatorTable} with the builtin
 * operators specific to the LucidDb personality
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LucidDbOperatorTable
    extends SqlStdOperatorTable
{
    //~ Static fields/initializers ---------------------------------------------

    private static LucidDbOperatorTable instance;

    private static LucidDbSpecialOperators specialOperators;

    public static final SqlFunction lcsRidFunc =
        new SqlFunction(
            "LCS_RID",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiAlwaysNullableBigint,
            null,
            SqlTypeStrategies.otcAny,
            SqlFunctionCategory.Numeric);

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the singleton, creating it if necessary.
     *
     * @return singleton with LucidDB-specific type
     */
    public static synchronized LucidDbOperatorTable ldbInstance()
    {
        if (instance == null) {
            instance = new LucidDbOperatorTable();
            instance.init();
            specialOperators = new LucidDbSpecialOperators();
        }

        return instance;
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance, creating it if necessary.
     *
     * @return singleton with generic type
     */
    public static SqlStdOperatorTable instance()
    {
        return ldbInstance();
    }

    public Set<SqlOperator> getSpecialOperators()
    {
        return specialOperators.getSpecialOperators();
    }

    public boolean isSpecialOperator(SqlOperator op)
    {
        return specialOperators.isSpecialOperator(op);
    }

    public boolean isSpecialColumnId(int colId)
    {
        return specialOperators.isSpecialColumnId(colId);
    }

    public String getSpecialOpName(SqlOperator op)
    {
        return specialOperators.getSpecialOpName(op);
    }

    public String getSpecialOpName(int colId)
    {
        return specialOperators.getSpecialOpName(colId);
    }

    public SqlTypeName getSpecialOpRetTypeName(int colId)
    {
        return specialOperators.getSpecialOpRetTypeName(colId);
    }

    public boolean isNullable(int colId)
    {
        return specialOperators.isNullable(colId);
    }

    public Integer getSpecialOpColumnId(SqlOperator op)
    {
        return specialOperators.getSpecialOpColumnId(op);
    }
}

// End LucidDbOperatorTable.java
