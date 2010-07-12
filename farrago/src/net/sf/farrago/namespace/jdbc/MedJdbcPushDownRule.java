/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 Dynamo BI Corporation
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
 * MedJdbcPushDownRule is a rule to push filters and projections down to Jdbc
 * sources.
 *
 * @author Sunny Choi
 * @version $Id$
 */
class MedJdbcPushDownRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a MedJdbcPushDownRule.
     *
     * @param operand Root operand, must not be null
     *
     * @param id Description of rule
     */
    public MedJdbcPushDownRule(RelOptRuleOperand operand, String id)
    {
        super(
            operand,
            "MedJdbcPushDownRule: " + id);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        boolean projOnFilter = false;
        boolean filterOnProj = false;
        boolean filterOnly = false;
        boolean projectOnly = false;
        if (description.contains("proj on filter")) {
            projOnFilter = true;
        } else if (description.contains("filter on proj")) {
            filterOnProj = true;
        } else if (description.contains("filter")) {
            filterOnly = true;
        } else {
            projectOnly = true;
        }

        int relLength = call.rels.length;
        final MedJdbcQueryRel queryRel =
            (MedJdbcQueryRel) call.rels[relLength - 1];

        // make sure we're starting from a plain
        // "select a, b, c from tbl"
        if (queryRel.getColumnSet() == null) {
            return;
        }
        SqlSelect origSelect = queryRel.getSql();
        SqlNodeList origSelectList = origSelect.getSelectList();
        for (SqlNode selectItem : origSelectList.getList()) {
            if (!(selectItem instanceof SqlIdentifier)) {
                return;
            }
        }
        if (!(origSelect.getFrom() instanceof SqlIdentifier)) {
            return;
        }
        if (origSelect.getGroup() != null) {
            return;
        }
        if (origSelect.getHaving() != null) {
            return;
        }
        if ((origSelect.getWhere() != null) && !projectOnly) {
            return;
        }
        if (origSelect.getWindowList().size() != 0) {
            return;
        }
        if (origSelect.getOrderList() != null) {
            return;
        }
        if (origSelect.isDistinct()) {
            return;
        }

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
            PushProjector pushProject =
                new PushProjector(
                    topProj,
                    null,
                    filter.getChild(),
                    PushProjector.ExprCondition.FALSE);
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
                PushProjector pushProject =
                    new PushProjector(
                        bottomProj,
                        null,
                        queryRel,
                        PushProjector.ExprCondition.FALSE);
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
            // push down filter, converting the RexCall to SqlNode using
            // RexToSqlNodeConverter
            RexToSqlNodeConverter exprConverter =
                new RexToSqlNodeConverterImpl(
                    new RexSqlStandardConvertletTable())
                {
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
            filterNode = exprConverter.convertNode(filter.getCondition());
            if (filterNode == null) {
                // could not convert
                return;
            }
        }

        List<SqlNode> projList = null;
        String [] fieldNames = null;
        Map<String, String> aliasMap = new HashMap<String, String>();
        RelDataType [] fieldTypes = null;
        List<RelDataTypeField> fields = null;

        // push down projection
        if (projOnFilter) {
            projList = new ArrayList<SqlNode>();
            fields = topProj.getRowType().getFieldList();
        } else if (!filterOnly) {
            projList = new ArrayList<SqlNode>();

            if (newTopProject != null) {
                boolean eliminateTopProj = true;
                RexNode [] projectExprs = newTopProject.getProjectExps();
                for (int i = 0; i < projectExprs.length; ++i) {
                    RexNode node = projectExprs[i];
                    if (!(node instanceof RexInputRef)) {
                        eliminateTopProj = false;
                        break;
                    }
                    String newFieldName =
                        newTopProject.getRowType().getFieldList()
                        .get(i).getName();
                    String oldFieldName =
                        bottomProj.getRowType().getFieldList()
                        .get(((RexInputRef) node).getIndex()).getName();
                    if (!newFieldName.equals(oldFieldName)) {
                        aliasMap.put(newFieldName, oldFieldName);
                    }
                }
                if (eliminateTopProj) {
                    fields = newTopProject.getRowType().getFieldList();
                    newTopProject = null;
                } else {
                    aliasMap.clear();
                }
            }

            if (fields == null) {
                fields = bottomProj.getRowType().getFieldList();
            }
        }

        SqlNodeList projection = origSelectList;
        if (projList != null) {
            int fieldLen = fields.size();
            fieldNames = new String[fieldLen];
            fieldTypes = new RelDataType[fieldLen];
            for (int i = 0; i < fieldLen; i++) {
                RelDataTypeField field = fields.get(i);
                String oldFieldName = aliasMap.get(field.getName());
                if (oldFieldName == null) {
                    projList.add(
                        new SqlIdentifier(
                            getSourceFieldName(queryRel, field.getName()),
                            SqlParserPos.ZERO));
                } else {
                    projList.add(
                        SqlStdOperatorTable.asOperator.createCall(
                            SqlParserPos.ZERO,
                            new SqlIdentifier(
                                getSourceFieldName(queryRel, oldFieldName),
                                SqlParserPos.ZERO),
                            new SqlIdentifier(
                                field.getName(),
                                SqlParserPos.ZERO)));
                }
                fieldNames[i] = field.getName();
                fieldTypes[i] = field.getType();
            }
            projection =
                new SqlNodeList(
                    Collections.unmodifiableList(projList),
                    SqlParserPos.ZERO);
        }

        SqlSelect selectWithFilter =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                projection,
                origSelect.getFrom(),
                projectOnly ? origSelect.getWhere() : filterNode,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);

        if (!queryRel.getServer().isRemoteSqlValid(selectWithFilter)) {
            return;
        }

        RelDataType rt = queryRel.getRowType();
        if (!filterOnly) {
            RexBuilder rexBuilder = queryRel.getCluster().getRexBuilder();
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            rt = typeFactory.createStructType(fieldTypes, fieldNames);
        }

        // TODO jvs 30-May-2009:  preserve unique key info where warranted
        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.getServer(),
                queryRel.getColumnSet(),
                queryRel.getCluster(),
                rt,
                queryRel.getConnection(),
                queryRel.getDialect(),
                selectWithFilter);

        if (newTopProject != null) {
            rel =
                new ProjectRel(
                    newTopProject.getCluster(),
                    rel,
                    newTopProject.getProjectExps(),
                    newTopProject.getRowType(),
                    newTopProject.getFlags(),
                    newTopProject.getCollationList());
        }

        call.transformTo(rel);
    }

    private String getSourceFieldName(MedJdbcQueryRel queryRel, String name)
    {
        String fieldName = name;
        if (!queryRel.getServer().lenient) {
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

// End MedJdbcPushDownRule.java
