/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
