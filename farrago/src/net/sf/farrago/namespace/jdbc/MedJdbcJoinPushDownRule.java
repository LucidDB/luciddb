/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
package net.sf.farrago.namespace.jdbc;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * MedJdbcJoinPushDownRule is a rule to push join down into
 * JDBC sources.
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcJoinPushDownRule
    extends RelOptRule
{
    public static final MedJdbcJoinPushDownRule instance =
        new MedJdbcJoinPushDownRule();

    private static final String LEFT_INPUT = "LEFT_INPUT";
    private static final String RIGHT_INPUT = "RIGHT_INPUT";

    public MedJdbcJoinPushDownRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY),
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY)));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        MedJdbcQueryRel leftRel = (MedJdbcQueryRel) call.rels[1];
        MedJdbcQueryRel rightRel = (MedJdbcQueryRel) call.rels[2];

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        SqlNode leftSelect = leftRel.getSql();
        SqlNode rightSelect = rightRel.getSql();
        MedJdbcDataServer combinedServer =
            leftRel.server.testQueryCombination(rightRel.server);
        if (combinedServer == null) {
            // Try it the other way in case the servers are asymmetric
            combinedServer =
                rightRel.server.testQueryCombination(leftRel.server);
        }
        if (combinedServer == null) {
            return;
        }

        // attempt to convert the join condition to SqlNode representation
        SqlNode onClause = null;
        SqlNode whereClause = null;
        RexNode rexJoinCond = joinRel.getCondition();
        if (!rexJoinCond.isAlwaysTrue()) {
            onClause = convertJoinCondition(rexJoinCond, leftRel, rightRel);
            if (onClause == null) {
                return;
            }
        }

        SqlJoinOperator.ConditionType
            conditionType = SqlJoinOperator.ConditionType.On;
        SqlJoinOperator.JoinType joinType;
        switch (joinRel.getJoinType()) {
        case INNER:
            conditionType = SqlJoinOperator.ConditionType.None;
            joinType = SqlJoinOperator.JoinType.Comma;
            whereClause = onClause;
            onClause = null;
            break;
        case LEFT:
            joinType = SqlJoinOperator.JoinType.Left;
            break;
        case RIGHT:
            joinType = SqlJoinOperator.JoinType.Right;
            break;
        case FULL:
            joinType = SqlJoinOperator.JoinType.Full;
            break;
        default:
            throw Util.needToImplement(joinRel.getJoinType());
        }
        SqlSelect selectWithJoin =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                // TODO:  remote source may not produce same
                // field names as us, so enumerate them explicitly
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                SqlStdOperatorTable.joinOperator.createCall(
                    SqlStdOperatorTable.asOperator.createCall(
                        SqlParserPos.ZERO,
                        leftSelect,
                        new SqlIdentifier(LEFT_INPUT, SqlParserPos.ZERO)),
                    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                    SqlLiteral.createSymbol(
                        joinType,
                        SqlParserPos.ZERO),
                    SqlStdOperatorTable.asOperator.createCall(
                        SqlParserPos.ZERO,
                        rightSelect,
                        new SqlIdentifier(RIGHT_INPUT, SqlParserPos.ZERO)),
                    SqlLiteral.createSymbol(
                        conditionType,
                        SqlParserPos.ZERO),
                    onClause,
                    SqlParserPos.ZERO),
                whereClause,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        if (!combinedServer.isRemoteSqlValid(selectWithJoin)) {
            return;
        }
        RelNode rel =
            new MedJdbcQueryRel(
                combinedServer,
                null,
                leftRel.getCluster(),
                joinRel.getRowType(),
                (combinedServer == leftRel.server)
                ? leftRel.getConnection() : rightRel.getConnection(),
                (combinedServer == leftRel.server)
                ? leftRel.getDialect() : rightRel.getDialect(),
                selectWithJoin);
        call.transformTo(rel);
    }

    private SqlNode convertJoinCondition(
        RexNode rexJoinCond,
        final MedJdbcQueryRel leftRel,
        final MedJdbcQueryRel rightRel)
    {
        RexToSqlNodeConverter exprConverter =
            new RexToSqlNodeConverterImpl(
                new RexSqlStandardConvertletTable())
            {
                public SqlIdentifier convertInputRef(RexInputRef ref)
                {
                    String [] names = new String[2];
                    int iField = ref.getIndex();
                    RelNode sourceRel;
                    if (iField < leftRel.getRowType().getFieldCount()) {
                        sourceRel = leftRel;
                        names[0] = LEFT_INPUT;
                    } else {
                        sourceRel = rightRel;
                        iField -= leftRel.getRowType().getFieldCount();
                        assert(iField < rightRel.getRowType().getFieldCount());
                        names[0] = RIGHT_INPUT;
                    }
                    names[1] = sourceRel.getRowType()
                        .getFieldList().get(iField).getName();
                    return new SqlIdentifier(
                        names,
                        SqlParserPos.ZERO);

                }
            };

        // Apply standard conversions.
        return exprConverter.convertNode(rexJoinCond);
    }
}

// End MedJdbcJoinPushDownRule.java
