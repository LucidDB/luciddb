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
package net.sf.farrago.namespace.jdbc;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.*;

import java.util.*;

/**
 * MedJdbcAggPushDownRule is a rule to push aggregations down into
 * JDBC sources.
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcAggPushDownRule
    extends RelOptRule
{
    public static final MedJdbcAggPushDownRule instance =
        new MedJdbcAggPushDownRule();

    public MedJdbcAggPushDownRule()
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY)));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        MedJdbcQueryRel queryRel = (MedJdbcQueryRel) call.rels[1];
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList groupBy = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < aggRel.getGroupCount(); ++i) {
            SqlIdentifier id =
                new SqlIdentifier(
                    queryRel.getRowType().getFieldList().get(i).getName(),
                    SqlParserPos.ZERO);
            selectList.add(id);
            groupBy.add(id);
        }
        for (AggregateCall aggCall : aggRel.getAggCallList()) {
            if (aggCall.isDistinct()) {
                // punt on DISTINCT...it will typically have been
                // rewritten away already
                return;
            }
            if (aggCall.getArgList().size() > 1) {
                return;
            }
            SqlAggFunction func = (SqlAggFunction) aggCall.getAggregation();
            SqlIdentifier id;
            if (aggCall.getArgList().size() == 1) {
                int iArg = aggCall.getArgList().get(0);
                id = new SqlIdentifier(
                    queryRel.getRowType().getFieldList().get(iArg).getName(),
                    SqlParserPos.ZERO);
            } else {
                id = new SqlIdentifier("*", SqlParserPos.ZERO);
            }
            SqlCall funcCall = func.createCall(SqlParserPos.ZERO, id);
            // no alias is needed since references are by position
            // rather than field name
            selectList.add(funcCall);
        }
        SqlSelect selectWithAgg =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                queryRel.getSql(),
                null,
                (groupBy.size() == 0) ? null : groupBy,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        if (!queryRel.getServer().isRemoteSqlValid(selectWithAgg)) {
            return;
        }

        // mark the aggregation key as unique; the planner may
        // rely on having this info available
        Set<BitSet> uniqueKeys = new HashSet<BitSet>();
        BitSet uniqueKey = new BitSet(aggRel.getRowType().getFieldCount());
        uniqueKey.set(0, aggRel.getGroupCount());
        uniqueKeys.add(uniqueKey);

        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.getServer(),
                queryRel.getColumnSet(),
                queryRel.getCluster(),
                aggRel.getRowType(),
                queryRel.getConnection(),
                queryRel.getDialect(),
                selectWithAgg,
                uniqueKeys);
        call.transformTo(rel);
    }
}

// End MedJdbcAggPushDownRule.java
