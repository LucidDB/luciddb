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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;


/**
 * MedJdbcPushDownRule is a rule to push filters and projections down to Jdbc source
 *
 * @author Sunny Choi
 * @version $Id$
 */
class MedJdbcPushDownRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    boolean projOnFilter = false;
    boolean filterOnProj = false;
    boolean filterOnly = false;
    boolean projectOnly = false;

    // ~ Constructors ---------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MedJdbcPushDownRule object.
     */

    public MedJdbcPushDownRule(RelOptRuleOperand rule, String id)
    {
        super(rule);
        description = "MedJdbcPushDownRule: " + id;
        if (description.contains("proj on filter")) {
            projOnFilter = true;
        } else if (description.contains("filter on proj")) {
            filterOnProj = true;
        } else if (description.contains("filter")) {
            filterOnly = true;
        } else {
            projectOnly = true;
        }
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods --------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        int relLength = call.rels.length;
        final MedJdbcQueryRel queryRel =
            (MedJdbcQueryRel) call.rels[relLength - 1];

        ProjectRel topProj = null;
        FilterRel filter = null;
        ProjectRel bottomProj = null;
        ProjectRel newTopProject = null;

        if (!projectOnly && !filterOnly) {
            filter = (FilterRel) call.rels[relLength - 3];
        } else if (filterOnly) {
            filter = (FilterRel) call.rels[relLength - 2];
        }

        if (projOnFilter) {
            topProj = (ProjectRel) call.rels[0];
            // handle any expressions in the projection
            PushProjector pushProject = new PushProjector(
                topProj, null, filter.getChild(), Collections.EMPTY_SET);
            ProjectRel newProj = pushProject.convertProject(null);
            if (newProj != null) {
                topProj = (ProjectRel) newProj.getChild();
                newTopProject = newProj;
            } else {
                // nothing to push down
                projOnFilter = false;
                newTopProject = topProj;
                filterOnProj = true;
            }
        }

        if (!filterOnly) {
            bottomProj = (ProjectRel) call.rels[relLength - 2];
            if (projectOnly) {
                PushProjector pushProject = new PushProjector(
                    bottomProj, null, queryRel, Collections.EMPTY_SET);
                ProjectRel newProj = pushProject.convertProject(null);
                if (newProj != null) {
                    bottomProj = (ProjectRel) newProj.getChild();
                    newTopProject = newProj;
                } else {
                    // only projection, nothing to push down
                    // could be second firing of this rule
                    return;
                }
            }
        }

        final FilterRel filterRel = filter;
        SqlNode filterNode = null;
        if (!projectOnly) {
            // push down filter
            RexCall filterCall = (RexCall) filter.getCondition();

            // convert the RexCall to SqlNode
            // using RexToSqlNodeConverter
            RexToSqlNodeConverter exprConverter =
                new RexToSqlNodeConverterImpl(
                    new RexSqlStandardConvertletTable()) {
                    public SqlIdentifier convertInputRef(RexInputRef ref)
                    {
                        RelDataType fields = filterRel.getRowType();
                        return new SqlIdentifier(
                            getSourceFieldName(
                                queryRel,
                                fields.getFieldList().get(
                                    ref.getIndex()).getName()),
                            SqlParserPos.ZERO);
                    }
                };

            // Apply standard conversions.
            try {
                filterNode = exprConverter.convertCall(filterCall);
            } catch (Exception e) {
                return;
            }
        }

        List<SqlIdentifier> projList = null;
        String[] fieldNames= null;
        RelDataType[] fieldTypes = null;

        // push down projection
        if (projOnFilter) {
            projList =
                new ArrayList<SqlIdentifier>();
            List<RelDataTypeField> fields =
                topProj.getRowType().getFieldList();
            int fieldLen = fields.size();
            fieldNames = new String[fieldLen];
            fieldTypes = new RelDataType[fieldLen];
            for (int i = 0; i < fieldLen; i++) {
                RelDataTypeField field = fields.get(i);
                projList.add(
                    new SqlIdentifier(
                        getSourceFieldName(queryRel, field.getName()),
                        SqlParserPos.ZERO));
                fieldNames[i] = field.getName();
                fieldTypes[i] = field.getType();
            }
        } else if (!filterOnly) {
            projList =
                new ArrayList<SqlIdentifier>();
            List<RelDataTypeField> fields =
                bottomProj.getRowType().getFieldList();
            int fieldLen = fields.size();
            fieldNames = new String[fieldLen];
            fieldTypes = new RelDataType[fieldLen];
            for (int i = 0; i < fieldLen; i++) {
                RelDataTypeField field = fields.get(i);
                projList.add(
                    new SqlIdentifier(
                        getSourceFieldName(queryRel, field.getName()),
                        SqlParserPos.ZERO));
                fieldNames[i] = field.getName();
                fieldTypes[i] = field.getType();
            }
        }

        SqlNodeList projection = queryRel.getSql().getSelectList();
        if (projList != null) {
            projection = new SqlNodeList(
                Collections.unmodifiableList(
                    projList),
                SqlParserPos.ZERO);
        }

        if (filterNode == null) {
            filterNode = queryRel.getSql().getWhere();
        }

        SqlSelect selectWithFilter =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                projection,
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

        // test if sql can be executed against source
        ResultSet rs = null;
        PreparedStatement ps = null;
        Statement testStatement = null;
        try {
            // Workaround for Oracle JDBC thin driver, where
            // PreparedStatement.getMetaData does not actually get metadata
            // before execution
            if (dialect.isOracle()) {
                String quotedSql = dialect.quoteStringLiteral(sql);
                String sqlTest =
                    " DECLARE"
                    + "   test_cursor integer;"
                    + " BEGIN"
                    + "   test_cursor := dbms_sql.open_cursor;"
                    + "   dbms_sql.parse(test_cursor, " + quotedSql + ", "
                    + "   dbms_sql.native);"
                    + "   dbms_sql.close_cursor(test_cursor);"
                    + " EXCEPTION"
                    + " WHEN OTHERS THEN"
                    + "   dbms_sql.close_cursor(test_cursor);"
                    + "   RAISE;"
                    + " END;";
                testStatement = server.getConnection().createStatement();
                rs = testStatement.executeQuery(sqlTest);
            } else {
                ps = server.getConnection().prepareStatement(sql);
                if (ps != null) {
                    if (ps.getMetaData() == null) {
                        return;
                    }
                }
            }
        } catch (SQLException ex) {
            return;
        } catch (RuntimeException ex) {
            return;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (testStatement != null) {
                    testStatement.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException sqe) {
            }
        }

        RelDataType rt = queryRel.getRowType();
        if (!filterOnly) {
            RexBuilder rexBuilder = queryRel.getCluster().getRexBuilder();
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            rt = typeFactory.createStructType(fieldTypes, fieldNames);
        }

        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.columnSet,
                queryRel.getCluster(),
                rt,
                queryRel.connection,
                queryRel.dialect,
                selectWithFilter);

        if (newTopProject != null) {
            rel = new ProjectRel(
                newTopProject.getCluster(),
                rel,
                newTopProject.getProjectExps(),
                newTopProject.getRowType(),
                newTopProject.getFlags(),
                newTopProject.getCollationList());
        }

        call.transformTo(rel);
    }

    private String getSourceFieldName(MedJdbcQueryRel queryRel, String name) {
        String fieldName = name;
        if (!queryRel.columnSet.directory.server.lenient) {
            List<RelDataTypeField> fieldList =
                queryRel.columnSet.origRowType.getFieldList();
            List<RelDataTypeField> srcFields =
                queryRel.columnSet.srcRowType.getFieldList();
            for (int i = 0; i < fieldList.size(); i++) {
                if (name.equals(fieldList.get(i).getName())) {
                    fieldName = srcFields.get(i).getName();
                    break;
                }
            }
        }
        return fieldName;
    }
}
//End MedJdbcPushDownRule.java
