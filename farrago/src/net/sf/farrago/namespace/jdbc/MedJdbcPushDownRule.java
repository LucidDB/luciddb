/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

import java.sql.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;

/**
 * MedJdbcPushDownRule is a rule to push filters down to Jdbc source
 *
 * @author Sunny Choi
 * @version $Id$
 */
class MedJdbcPushDownRule extends RelOptRule
{
    boolean projOnFilter = false;
    boolean filterOnProj = false;
    boolean filterOnly = false;

    // ~ Constructors ---------------------------------------------------------

    /**
     * Creates a new MedJdbcPushDownRule object.
     */

    public MedJdbcPushDownRule(RelOptRuleOperand rule, String id)
    {
        super(rule);
        description = "MedJdbcPushDownRule: " + id;
        if (description.contains("proj on filter")) {
            projOnFilter = true;
        }
        if (description.contains("filter on proj")) {
            filterOnProj = true;
        } else {
            filterOnly = true;
        }
    }

    // ~ Methods --------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        int relLength = call.rels.length;
        final MedJdbcQueryRel queryRel =
            (MedJdbcQueryRel) call.rels[relLength - 1];

        if (queryRel.isPushdownDone()) {
            return;
        }

        ProjectRel topProj = null;
        FilterRel filter = null;
        ProjectRel bottomProj = null;

        if (projOnFilter) {
            topProj = (ProjectRel) call.rels[0];
        }
        if (filterOnProj) {
            bottomProj = (ProjectRel) call.rels[relLength - 2];
            filter = (FilterRel) call.rels[relLength - 3];
        } else {
            filter = (FilterRel) call.rels[relLength - 2];
        }

        // push down filter
        RexCall filterCall = (RexCall) filter.getCondition();

        // convert the RexCall to SqlNode
        // RexToSqlConverter
        RexToSqlNodeConverter exprConverter =
            new RexToSqlNodeConverterImpl(
                new RexSqlStandardConvertletTable());

        // Apply standard conversions.
        SqlNode filterNode;
        try {
            filterNode =
                exprConverter.convertCall(
                    new RexInputRefToSqlNodeConverter()
                    {
                        public SqlIdentifier convertInputRef(RexInputRef ref) {
                            if (queryRel.columnSet.directory.server.lenient) {
                                RelDataType fields =
                                    queryRel.columnSet.getRowType();
                                String fieldName = fields.getFieldList().
                                    get(ref.getIndex()).getName();
                                return new SqlIdentifier(
                                    fieldName, SqlParserPos.ZERO);
                            } else {
                                RelDataType fields =
                                    queryRel.columnSet.srcRowType;
                                String fieldName = fields.getFieldList().
                                    get(ref.getIndex()).getName();
                                return new SqlIdentifier(
                                    fieldName, SqlParserPos.ZERO);
                            }
                        }
                    },
                    filterCall);
        } catch (Exception e) {
            return;
        }

        SqlSelect selectWithFilter =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                queryRel.getSql().getSelectList(),
                queryRel.getSql().getFrom(),
                filterNode,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);

        MedJdbcDataServer server = queryRel.columnSet.directory.server;
        SqlDialect dialect = new SqlDialect(server.databaseMetaData);
        String sql = selectWithFilter.toSqlString(dialect);
        sql = queryRel.columnSet.directory.normalizeQueryString(sql);

        try {
            PreparedStatement ps =
                server.getConnection().prepareStatement(sql);
            if (ps != null) {
                ps.getMetaData();
            }
        } catch (SQLException ex) {
//            return;
        }

        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.columnSet,
                queryRel.getCluster(),
                queryRel.getRowType(),
                queryRel.connection,
                queryRel.dialect,
                selectWithFilter);

        ((MedJdbcQueryRel)rel).setPushdownDone();

        if (bottomProj != null) {
            rel = new ProjectRel(
                bottomProj.getCluster(),
                rel,
                bottomProj.getProjectExps(),
                bottomProj.getRowType(),
                bottomProj.getFlags(),
                bottomProj.getCollationList());
        }

        rel = new FilterRel(
            filter.getCluster(),
            rel,
            filter.getCondition());

        if (topProj != null) {
            rel = new ProjectRel(
                topProj.getCluster(),
                rel,
                topProj.getProjectExps(),
                topProj.getRowType(),
                topProj.getFlags(),
                topProj.getCollationList());
        }

        call.transformTo(rel);
    }

}
//End MedJdbcPushDownRule.java
