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
package net.sf.farrago.namespace.mql;

import java.util.*;
import java.text.*;
import java.math.*;
import java.io.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

/**
 * MedMqlPushDownRule is an optimizer rule to push filters and
 * projections down into MQL.
 *
 * @author John Sichi
 * @version $Id$
 */
class MedMqlPushDownRule extends RelOptRule
{
    /**
     * Creates a MedMqlPushDownRule.
     */
    public MedMqlPushDownRule(RelOptRuleOperand operand, String id)
    {
        super(operand, "MedMqlPushDownRule: " + id);
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        // TODO jvs 7-Jan-2009:  factor out code shared with
        // MedJdbcPushDownRule into net.sf.farrago.namespace.impl

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
        } else if (description.contains("proj")) {
            projectOnly = true;
        }

        int relLength = call.rels.length;
        MedMqlTableRel tableRel = (MedMqlTableRel) call.rels[relLength - 1];

        if (relLength == 1) {
            // no filter, no project, no problem
            transformToFarragoUdxRel(
                call, tableRel, null, null, null);
            return;
        }

        ProjectRel topProj = null;
        ProjectRel origTopProj = null;
        FilterRel filter = null;
        ProjectRel bottomProj = null;
        ProjectRel origBottomProj = null;
        ProjectRel newTopProject = null;

        if (!projectOnly && !filterOnly) {
            filter = (FilterRel) call.rels[relLength - 3];
        } else if (filterOnly) {
            filter = (FilterRel) call.rels[relLength - 2];
        }

        if (projOnFilter) {
            topProj = (ProjectRel) call.rels[0];
            origTopProj = topProj;
            // handle any expressions in the projection
            PushProjector pushProject = new PushProjector(
                topProj, null, filter.getChild(),
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
            origBottomProj = bottomProj;
            if (projectOnly) {
                PushProjector pushProject = new PushProjector(
                    bottomProj, null, tableRel,
                    PushProjector.ExprCondition.FALSE);
                ProjectRel newProj = pushProject.convertProject(null);
                if (newProj != null) {
                    bottomProj = (ProjectRel) newProj.getChild();
                    newTopProject = newProj;
                }
            }
        }

        RexBuilder rexBuilder = tableRel.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        SqlOperator op = null;
        RexNode[] operands = null;

        if (!projectOnly) {
            RexCall filterCall = (RexCall) filter.getCondition();
            op = filterCall.getOperator();
            operands = filterCall.getOperands();
        }

        Map<String, String> fieldBindings = createFieldBindings(tableRel);

        String[] allOrigFieldNames =
            RelOptUtil.getFieldNames(tableRel.getRowType());

        RelDataType[] finalRowTypes =
            new RelDataType[allOrigFieldNames.length];

        String[] fieldNames = allOrigFieldNames;

        if (!filterOnly) {
            fieldBindings = new TreeMap<String, String>();
            RelDataTypeField[] projFields = bottomProj.getRowType().getFields();
            fieldNames = new String[projFields.length];
            RexNode[] nodes = bottomProj.getChildExps();
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] instanceof RexInputRef) {
                    int x = ((RexInputRef)nodes[i]).getIndex();
                    fieldNames[i] = allOrigFieldNames[x];
                } else {
                    // can't handle
                    transformToFarragoUdxRel(
                        call, tableRel, filter, origTopProj, origBottomProj);
                    return;
                }
            }
            finalRowTypes = new RelDataType[fieldNames.length];
            for (int i = 0; i < projFields.length; i++) {
                finalRowTypes[i] = projFields[i].getType();
                fieldBindings.put(projFields[i].getName(), null);
            }
        }

        if (!validProjection(fieldNames)) {
            transformToFarragoUdxRel(
                call, tableRel, filter, origTopProj, origBottomProj);
            return;
        }

        // TODO jvs 8-Jan-2009:  In the case of a query like
        //   select "name" from metaweb.artists where "id" = '/en/gene_kelly';
        // we don't push down the projection because we don't find
        // out that we don't need the "id" field for filter
        // post-processing until it's too late.  The filter pushdown
        // needs to eliminate the projections which become
        // redundant.

        RexNode filterRemainder = null;
        if (filter != null) {
            filterRemainder = translateFilter(
                filter, fieldNames, fieldBindings);
        }

        RelDataType resultType;
        if (!filterOnly) {
            RelDataType newRowType =
                typeFactory.createStructType(finalRowTypes, fieldNames);
            resultType =
                typeFactory.createTypeWithNullability(newRowType, true);
        } else {
            resultType = typeFactory.createTypeWithNullability(
                tableRel.getRowType(), true);
        }
        RelNode rel = createFarragoUdxRel(
            tableRel,
            resultType,
            fieldBindings);

        if (filterRemainder != null) {
            rel = new FilterRel(rel.getCluster(), rel, filterRemainder);
        }

        if (origTopProj != null) {
            rel = new ProjectRel(
                origTopProj.getCluster(),
                rel,
                origTopProj.getProjectExps(),
                origTopProj.getRowType(),
                origTopProj.getFlags(),
                origTopProj.getCollationList());
        }

        if (projectOnly && newTopProject != null) {
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

    // TODO jvs 8-Jan-2009:  use org.eigenbase.sarg for comprehensive
    // filter translation
    private RexNode translateFilter(
        FilterRel filterRel,
        String [] fieldNames,
        Map<String, String> fieldBindings)
    {
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        List<RexNode> nodeList = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(filterRel.getCondition(), nodeList);
        Iterator<RexNode> iter = nodeList.iterator();
        while (iter.hasNext()) {
            RexNode node = iter.next();
            if (!(node instanceof RexCall)) {
                continue;
            }
            RexCall call = (RexCall) node;
            if (call.getOperator() != SqlStdOperatorTable.equalsOperator) {
                continue;
            }
            final RexNode [] operands = call.getOperands();
            RexNode op0 = operands[0];
            RexNode op1 = operands[1];
            if ((op1 instanceof RexInputRef) && (op0 instanceof RexLiteral)) {
                RexNode tmp = op0;
                op0 = op1;
                op1 = tmp;
            }
            if ((op0 instanceof RexInputRef) && (op1 instanceof RexLiteral)) {
                iter.remove();
                RexInputRef inputRef = (RexInputRef) op0;
                String fieldName = fieldNames[inputRef.getIndex()];
                RexLiteral literal = (RexLiteral) op1;
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                // REVIEW jvs 8-Jan-2009:  study Java vs JSON conventions
                literal.printAsJava(pw);
                pw.close();
                String value = sw.toString();
                fieldBindings.put(fieldName, value);
            }
        }
        if (nodeList.isEmpty()) {
            return null;
        }
        RexNode remainder =
            RexUtil.andRexNodeList(rexBuilder, nodeList);
        return remainder;
    }

    private boolean validProjection(String[] fieldNames)
    {
        // detect duplicate names
        Set<String> names = new HashSet<String>();
        for (String fieldName : fieldNames) {
            if (!names.add(fieldName)) {
                // conflict
                return false;
            }
        }
        return true;
    }

    private Map<String, String> createFieldBindings(
        MedMqlTableRel tableRel)
    {
        Map<String, String> fieldBindings =
            new TreeMap<String, String>();
        for (RelDataTypeField field : tableRel.getRowType().getFieldList()) {
            fieldBindings.put(field.getName(), null);
        }
        return fieldBindings;
    }

    private RelNode createFarragoUdxRel(
        MedMqlTableRel tableRel,
        RelDataType rowType,
        Map<String, String> fieldBindings)
    {
        MedMqlColumnSet columnSet = tableRel.getMedMqlColumnSet();

        RexBuilder rexBuilder = tableRel.getCluster().getRexBuilder();

        // FIXME jvs 7-Jan-2008: escape quotes in field names; use a proper
        // JSON library
        StringBuilder sbMql = new StringBuilder();
        sbMql.append("{\"query\":");
        sbMql.append("[{\"type\":\"");
        sbMql.append(columnSet.metawebType);
        sbMql.append("\"");
        for (Map.Entry<String, String> entry : fieldBindings.entrySet()) {
            sbMql.append(",\"");
            sbMql.append(entry.getKey());
            sbMql.append("\":");
            if (entry.getValue() == null) {
                sbMql.append("null");
            } else {
                sbMql.append(entry.getValue());
            }
        }
        sbMql.append("}]");
        sbMql.append("}");
        String mql = sbMql.toString();

        StringBuilder sbRowType = new StringBuilder();
        boolean first = true;
        for (RelDataTypeField field : rowType.getFields()) {
            if (first) {
                first = false;
            } else {
                sbRowType.append("|");
            }
            sbRowType.append(field.getName());
        }
        String rowTypeString = sbRowType.toString();

        RexNode urlArg = rexBuilder.makeLiteral(columnSet.server.getUrl());
        RexNode mqlArg = rexBuilder.makeLiteral(mql);
        RexNode rowTypeArg = rexBuilder.makeLiteral(rowTypeString);

        RelNode rel = FarragoJavaUdxRel.newUdxRel(
            columnSet.getPreparingStmt(),
            rowType,
            columnSet.udxSpecificName,
            columnSet.server.getServerMofId(),
            new RexNode[] { urlArg, mqlArg, rowTypeArg },
            RelNode.emptyArray);
        return rel;
    }

    private void transformToFarragoUdxRel(
        RelOptRuleCall call,
        MedMqlTableRel tableRel,
        FilterRel filter, ProjectRel topProj, ProjectRel bottomProj)
    {
        MedMqlColumnSet columnSet = tableRel.getMedMqlColumnSet();

        RelNode rel = createFarragoUdxRel(
            tableRel,
            tableRel.getRowType(),
            createFieldBindings(tableRel));

        if (bottomProj != null) {
            rel = new ProjectRel(
                bottomProj.getCluster(),
                rel,
                bottomProj.getProjectExps(),
                bottomProj.getRowType(),
                bottomProj.getFlags(),
                bottomProj.getCollationList());
        }

        if (filter != null) {
            rel = new FilterRel(
                filter.getCluster(),
                rel,
                filter.getCondition());
        }

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

// End MedMqlPushDownRule.java
