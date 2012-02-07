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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * MedJdbcProjectionPushDownRule is a rule to push projection down into
 * JDBC sources (wrapping rather than rewriting the foreign SQL; for
 * filter/projection rewrite, see {@link MedJdbcPushDownRule}).
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcProjectionPushDownRule
    extends RelOptRule
{
    public static final MedJdbcProjectionPushDownRule instance =
        new MedJdbcProjectionPushDownRule();

    public MedJdbcProjectionPushDownRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY)));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel projRel = (ProjectRel) call.rels[0];
        final MedJdbcQueryRel queryRel = (MedJdbcQueryRel) call.rels[1];

        RexToSqlNodeConverter exprConverter =
            new RexToSqlNodeConverterImpl(
                new RexSqlStandardConvertletTable())
            {
                public SqlIdentifier convertInputRef(RexInputRef ref)
                {
                    RelDataType fields = queryRel.getRowType();
                    return new SqlIdentifier(
                        fields.getFieldList().get(ref.getIndex()).getName(),
                        SqlParserPos.ZERO);
                }
            };

        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        int iField = 0;
        for (RexNode rexNode : projRel.getProjectExps()) {
            SqlNode sqlNode = exprConverter.convertNode(rexNode);
            if (sqlNode == null) {
                // could not translate
                return;
            }
            String alias =
                projRel.getRowType().getFieldList().get(iField++).getName();
            sqlNode =
                SqlStdOperatorTable.asOperator.createCall(
                    SqlParserPos.ZERO,
                    sqlNode,
                    new SqlIdentifier(
                        alias,
                        SqlParserPos.ZERO));
            selectList.add(sqlNode);
        }

        SqlSelect selectWithProj =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                queryRel.getSql(),
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        if (!queryRel.getServer().isRemoteSqlValid(selectWithProj)) {
            return;
        }

        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.getServer(),
                queryRel.getColumnSet(),
                queryRel.getCluster(),
                projRel.getRowType(),
                queryRel.getConnection(),
                queryRel.getDialect(),
                selectWithProj,
                null);
        call.transformTo(rel);
    }
}

// End MedJdbcProjectionPushDownRule.java
