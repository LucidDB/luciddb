/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.query;

import com.lucidera.farrago.*;

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
