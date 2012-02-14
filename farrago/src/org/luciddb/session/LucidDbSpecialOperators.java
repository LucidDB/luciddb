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

import org.luciddb.session.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * LucidDbSpecialOperators maintains information about builtin operators that
 * require special handling, e.g., LCS_RID
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LucidDbSpecialOperators
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Special column id values. Additional values should be appended here with
     * sequential id values.
     */
    private static final int LcsRidColumnId = 0x7FFFFF00;

    //~ Instance fields --------------------------------------------------------

    /**
     * List of the special operators
     */
    private Set<SqlOperator> specialOperators;

    /**
     * Maps the special operator to an object containing information about the
     * operator
     */
    private Map<SqlOperator, SpecialOperatorInfo> sqlOpMap;

    /**
     * Maps the special column id representing a special operator to the object
     * containing information about the operator
     */
    private Map<Integer, SpecialOperatorInfo> colIdMap;

    //~ Constructors -----------------------------------------------------------

    public LucidDbSpecialOperators()
    {
        specialOperators = new HashSet<SqlOperator>();
        specialOperators.add(LucidDbOperatorTable.lcsRidFunc);

        final SpecialOperatorInfo lcsRidFuncInfo =
            new SpecialOperatorInfo(
                LucidDbOperatorTable.lcsRidFunc,
                SqlTypeName.BIGINT,
                true,
                LcsRidColumnId);

        sqlOpMap = new HashMap<SqlOperator, SpecialOperatorInfo>();
        sqlOpMap.put(LucidDbOperatorTable.lcsRidFunc, lcsRidFuncInfo);

        colIdMap = new HashMap<Integer, SpecialOperatorInfo>();
        colIdMap.put(LcsRidColumnId, lcsRidFuncInfo);
    }

    //~ Methods ----------------------------------------------------------------

    public Set<SqlOperator> getSpecialOperators()
    {
        return specialOperators;
    }

    public boolean isSpecialOperator(SqlOperator op)
    {
        return specialOperators.contains(op);
    }

    public boolean isSpecialColumnId(int colId)
    {
        return (colIdMap.get(colId) != null);
    }

    public String getSpecialOpName(SqlOperator op)
    {
        return op.getName();
    }

    public String getSpecialOpName(int colId)
    {
        SpecialOperatorInfo opInfo = colIdMap.get(colId);
        if (opInfo == null) {
            return null;
        } else {
            return opInfo.getFuncName();
        }
    }

    public SqlTypeName getSpecialOpRetTypeName(int colId)
    {
        SpecialOperatorInfo opInfo = colIdMap.get(colId);
        if (opInfo == null) {
            return null;
        } else {
            return opInfo.getRetType();
        }
    }

    public Integer getSpecialOpColumnId(SqlOperator op)
    {
        SpecialOperatorInfo opInfo = sqlOpMap.get(op);
        if (opInfo == null) {
            return null;
        } else {
            return opInfo.getColId();
        }
    }

    public Boolean isNullable(int colId)
    {
        SpecialOperatorInfo opInfo = colIdMap.get(colId);
        if (opInfo == null) {
            return null;
        } else {
            return opInfo.isNullable();
        }
    }

    public static boolean isLcsRidColumnId(int colId)
    {
        return colId == LcsRidColumnId;
    }

    /**
     * Creates an expression corresponding to an lcs rid
     *
     * @param rexBuilder rex builder used to create the expression
     * @param rel the relnode that the rid expression corresponds to
     * @param fieldNo field in the relnode to use as the argument to the rid
     * expression
     *
     * @return the rid expression
     */
    public static RexNode makeRidExpr(
        RexBuilder rexBuilder,
        RelNode rel,
        int fieldNo)
    {
        RexNode ridArg =
            rexBuilder.makeInputRef(
                rel.getRowType().getFields()[fieldNo].getType(),
                fieldNo);
        return rexBuilder.makeCall(LucidDbOperatorTable.lcsRidFunc, ridArg);
    }

    public static RexNode makeRidExpr(
        RexBuilder rexBuilder,
        RelNode rel)
    {
        // arbitrarily use the first column from the table as the argument
        // to the function
        return makeRidExpr(rexBuilder, rel, 0);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class SpecialOperatorInfo
    {
        private SqlOperator op;
        private SqlTypeName typeName;
        private boolean nullable;
        private int colId;

        public SpecialOperatorInfo(
            SqlOperator op,
            SqlTypeName typeName,
            boolean nullable,
            int colId)
        {
            this.op = op;
            this.typeName = typeName;
            this.nullable = nullable;
            this.colId = colId;
        }

        public SqlTypeName getRetType()
        {
            return typeName;
        }

        public boolean isNullable()
        {
            return nullable;
        }

        public String getFuncName()
        {
            return op.getName();
        }

        public int getColId()
        {
            return colId;
        }
    }
}

// End LucidDbSpecialOperators.java
