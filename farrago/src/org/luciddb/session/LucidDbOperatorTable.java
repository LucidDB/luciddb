/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
