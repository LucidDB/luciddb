/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
class MedJdbcAggPushDownRule
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
        MedJdbcNameDirectory dir = queryRel.columnSet.directory;
        if (!dir.isRemoteSqlValid(selectWithAgg)) {
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
                queryRel.columnSet,
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
