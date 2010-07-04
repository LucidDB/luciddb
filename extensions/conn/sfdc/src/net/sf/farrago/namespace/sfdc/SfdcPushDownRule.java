/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */
package net.sf.farrago.namespace.sfdc;

import java.math.BigDecimal;

import java.text.*;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;


/**
 * SfdcPushDownRule is a rule to push filters and projections to SFDC
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcPushDownRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    // ~ Constructors ---------------------------------------------------------

    /**
     * Creates a new SfdcPushDownRule object.
     */
    public SfdcPushDownRule(RelOptRuleOperand rule, String id)
    {
        super(rule, "SfdcPushDownRule: " + id);
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods --------------------------------------------------------------

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
        SfdcUdxRel udxRel = (SfdcUdxRel) call.rels[relLength - 1];
        if ((!udxRel.getUdx().getName().equalsIgnoreCase("sfdc_query"))
            && !udxRel.getUdx().getName().equalsIgnoreCase("query"))
        {
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
            origBottomProj = bottomProj;
            if (projectOnly) {
                PushProjector pushProject =
                    new PushProjector(
                        bottomProj,
                        null,
                        udxRel,
                        PushProjector.ExprCondition.FALSE);
                ProjectRel newProj = pushProject.convertProject(null);
                if (newProj != null) {
                    bottomProj = (ProjectRel) newProj.getChild();
                    newTopProject = newProj;
                }
            }
        }

        RexBuilder rexBuilder = udxRel.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        SqlOperator op = null;
        RexNode [] operands = null;
        String filterString = null;

        if (!projectOnly) {
            RexCall filterCall = (RexCall) filter.getCondition();
            op = filterCall.getOperator();
            operands = filterCall.getOperands();
        }

        RelDataTypeField [] projFields = null;
        String allOrigFields = udxRel.getTable().fields;
        String [] allOrigFieldNames = allOrigFields.split(",");

        String [] fieldTypes = new String[allOrigFieldNames.length];
        RelDataType [] finalRowTypes =
            new RelDataType[allOrigFieldNames.length];
        String types = null;

        String fields = allOrigFields;
        String [] fieldNames = allOrigFieldNames;

        if (!filterOnly) {
            projFields = bottomProj.getRowType().getFields();
            fieldNames = new String[projFields.length];
            RexNode [] nod = bottomProj.getChildExps();
            for (int i = 0; i < nod.length; i++) {
                if (nod[i] instanceof RexInputRef) {
                    int x = ((RexInputRef) nod[i]).getIndex();
                    fieldNames[i] = allOrigFieldNames[x];
                } else {
                    // can't handle
                    transformToFarragoUdxRel(
                        call,
                        udxRel,
                        filter,
                        origTopProj,
                        origBottomProj);
                    return;
                }
            }
            finalRowTypes = new RelDataType[fieldNames.length];
            for (int i = 0; i < projFields.length; i++) {
                fieldTypes[i] = projFields[i].getType().toString();
                finalRowTypes[i] = projFields[i].getType();
                if (i == 0) {
                    fields = fieldNames[i];
                    types = projFields[i].getType().toString();
                } else {
                    fields = fields.concat(", " + fieldNames[i]);
                    types =
                        types.concat(
                            ","
                            + projFields[i].getType().toString());
                }
            }
        }

        String pushDownProj = fields;
        String pushDownProjTypes = types;
        String [] finalFieldNames = fieldNames;

        if (!validProjection(finalFieldNames)) {
            transformToFarragoUdxRel(
                call,
                udxRel,
                filter,
                origTopProj,
                origBottomProj);
            return;
        }

        if (!projectOnly) {
            filterString = printFilter(op, operands, "", fieldNames);
            if (filterString == null) {
                // can't handle - do projection only
                if (filterOnly) {
                    transformToFarragoUdxRel(
                        call,
                        udxRel,
                        filter,
                        origTopProj,
                        origBottomProj);
                    return;
                }
            }
        }

        RexCall origQuery = (RexCall) udxRel.getCall();
        RexLiteral origStrLit =
            (RexLiteral)
            ((RexCall) ((RexCall) (origQuery.getOperands()[0]))
            .getOperands()[0])
            .getOperands()[0];
        String origStr = ((NlsString) origStrLit.getValue()).getValue();
        origStr = origStr.substring(origStr.lastIndexOf(" from"));
        String newQuery = "";
        if (!projectOnly && (filterString != null)) {
            newQuery =
                "select " + pushDownProj + origStr + " where "
                + filterString;
        } else {
            newQuery = "select " + pushDownProj + origStr;
        }

        RexNode queryWithFilter = rexBuilder.makeLiteral(newQuery);
        RexNode [] args2;
        RelDataType resultType;
        if (!filterOnly) {
            RexNode queryTypes = rexBuilder.makeLiteral(pushDownProjTypes);
            args2 = new RexNode[] { queryWithFilter, queryTypes };
            RelDataType newRowType =
                typeFactory.createStructType(
                    finalRowTypes,
                    finalFieldNames);
            resultType =
                typeFactory.createTypeWithNullability(newRowType, true);
        } else {
            args2 =
                new RexNode[] {
                    queryWithFilter,
                    ((RexCall) (origQuery.getOperands()[0])).getOperands()[1]
                };
            resultType =
                typeFactory.createTypeWithNullability(
                    udxRel.getRowType(),
                    true);
        }
        RexNode rexCall = rexBuilder.makeCall(udxRel.getUdx(), args2);

        RelNode rel =
            new FarragoJavaUdxRel(
                udxRel.getCluster(),
                rexCall,
                resultType,
                udxRel.getServerMofId(),
                RelNode.emptyArray);

        rel = RelOptUtil.createCastRel(rel, resultType, true);

        if ((filter != null) && (filterString == null)) {
            rel = new FilterRel(rel.getCluster(), rel, filter.getCondition());
        }

        if (origTopProj != null) {
            rel =
                new ProjectRel(
                    origTopProj.getCluster(),
                    rel,
                    origTopProj.getProjectExps(),
                    origTopProj.getRowType(),
                    origTopProj.getFlags(),
                    origTopProj.getCollationList());
        }

        if (projectOnly && (newTopProject != null)) {
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
        return;
    }

    // ~ Private Methods ------------------------------------------------------

    private String printFilter(
        SqlOperator op,
        RexNode [] operands,
        String s,
        String [] fieldNames)
    {
        boolean isNullOp = false;
        boolean isNotNullOp = false;
        if (!valid(op.getKind())) {
            if (isNullOp(op)) {
                isNullOp = true;
            }
            if (isNotNullOp(op)) {
                isNotNullOp = true;
            }
            if (!isNullOp && !isNotNullOp) {
                return null;
            }
        }

        if (op.getName().equalsIgnoreCase("NOT")) {
            s = s.concat(" NOT ");
        }
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof RexCall) {
                s = s.concat("(");
                s = printFilter(
                    ((RexCall) operands[i]).getOperator(),
                    ((RexCall) operands[i]).getOperands(),
                    s,
                    fieldNames);
                if (s == null) {
                    return null;
                }
                s = s.concat(")");
                if (i != (operands.length - 1)) {
                    s = s.concat(" " + op.toString() + " ");
                }
            } else {
                if ((operands.length != 2) && !isNullOp && !isNotNullOp) {
                    return null;
                }
                if (operands[i] instanceof RexInputRef) {
                    String ordinal = ((RexInputRef) operands[i]).getName();
                    ordinal = ordinal.substring(1); // rid of the $
                    try {
                        Integer.valueOf(ordinal);
                    } catch (NumberFormatException ne) {
                        return null;
                    }
                    String name = fieldNames[Integer.valueOf(ordinal)];
                    s = s.concat(name);
                } else { // RexLiteral
                    RexLiteral lit = (RexLiteral) operands[i];
                    SqlTypeName litSqlType = lit.getTypeName();
                    if (litSqlType.equals(SqlTypeName.CHAR)) {
                        s = s.concat(
                            "'"
                            + ((NlsString) lit.getValue()).getValue().toString()
                            + "'");
                    } else if (
                        litSqlType.equals(SqlTypeName.TIMESTAMP)
                        || litSqlType.equals(SqlTypeName.DATE))
                    {
                        SimpleDateFormat sdf;
                        if (litSqlType.equals(SqlTypeName.TIMESTAMP)) {
                            sdf =
                                new SimpleDateFormat(
                                    "yyyy-MM-dd'T'HH:mm:ss'Z'");
                        } else {
                            sdf = new SimpleDateFormat("yyyy-MM-dd");
                        }
                        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                        StringBuffer sbuf = new StringBuffer();
                        String time =
                            sdf.format(
                                ((Calendar) ((RexLiteral) lit).getValue())
                                .getTime(),
                                sbuf,
                                new FieldPosition(0)).toString();
                        s = s.concat(time);
                    } else if (
                        litSqlType.equals(SqlTypeName.DECIMAL)
                        || litSqlType.equals(SqlTypeName.DOUBLE))
                    {
                        if (litSqlType.equals(SqlTypeName.DECIMAL)) {
                            s = s.concat(
                                ((BigDecimal) lit.getValue()).toBigInteger()
                                                             .toString());
                        } else {
                            s = s.concat(
                                ((BigDecimal) lit.getValue()).toString());
                        }
                    }
                }
                if (i == 0) {
                    if (isNullOp) {
                        s = s.concat(" = null");
                    } else {
                        if (isNotNullOp) {
                            s = s.concat(" != null");
                        } else {
                            s = s.concat(" " + op.toString() + " ");
                        }
                    }
                }
            }
        }
        return s;
    }

    // REVIEW jvs 30-May-2007: Wouldn't it be cleaner just
    // to have two separate methods (one for SqlKind, one for RexKind)?
    private boolean valid(Object kind)
    {
        if (kind instanceof SqlKind) {
            if (kind.equals(SqlKind.Equals)
                || kind.equals(SqlKind.NotEquals)
                || kind.equals(SqlKind.LessThan)
                || kind.equals(SqlKind.LessThanOrEqual)
                || kind.equals(SqlKind.GreaterThan)
                || kind.equals(SqlKind.GreaterThanOrEqual)
                || kind.equals(SqlKind.Like)
                || kind.equals(SqlKind.And)
                || kind.equals(SqlKind.Or)
                || kind.equals(SqlKind.Not))
            {
                return true;
            }
        } else {
            if (kind.equals(RexKind.Equals)
                || kind.equals(RexKind.NotEquals)
                || kind.equals(RexKind.LessThan)
                || kind.equals(RexKind.LessThanOrEqual)
                || kind.equals(RexKind.GreaterThan)
                || kind.equals(RexKind.GreaterThanOrEqual)
                || kind.equals(RexKind.Like)
                || kind.equals(RexKind.And)
                || kind.equals(RexKind.Or)
                || kind.equals(RexKind.Not))
            {
                return true;
            }
        }
        return false;
    }

    private boolean isNullOp(SqlOperator op)
    {
        if (op.equals(SqlStdOperatorTable.isNullOperator)) {
            return true;
        }
        return false;
    }

    private boolean isNotNullOp(SqlOperator op)
    {
        if (op.equals(SqlStdOperatorTable.isNotNullOperator)) {
            return true;
        }
        return false;
    }

    private void transformToFarragoUdxRel(
        RelOptRuleCall call,
        SfdcUdxRel udxRel,
        FilterRel filter,
        ProjectRel topProj,
        ProjectRel bottomProj)
    {
        RelNode rel =
            new FarragoJavaUdxRel(
                udxRel.getCluster(),
                udxRel.getCall(),
                udxRel.getRowType(),
                udxRel.getServerMofId());

        rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

        if (bottomProj != null) {
            rel =
                new ProjectRel(
                    bottomProj.getCluster(),
                    rel,
                    bottomProj.getProjectExps(),
                    bottomProj.getRowType(),
                    bottomProj.getFlags(),
                    bottomProj.getCollationList());
        }

        if (filter != null) {
            rel =
                new FilterRel(filter.getCluster(), rel, filter.getCondition());
        }

        if (topProj != null) {
            rel =
                new ProjectRel(
                    topProj.getCluster(),
                    rel,
                    topProj.getProjectExps(),
                    topProj.getRowType(),
                    topProj.getFlags(),
                    topProj.getCollationList());
        }
        call.transformTo(rel);
    }

    protected static boolean validProjection(String [] fieldNames)
    {
        // 1. no fields can be projected multiple times
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldToCheck = fieldNames[i];
            for (int j = i + 1; j < fieldNames.length; j++) {
                if (fieldToCheck.equals(fieldNames[j])) {
                    return false;
                }
            }
        }

        // 2. if projecting "Id", must be first
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase("Id") && (i != 0)) {
                return false;
            }
        }
        return true;
    }
}

// End SfdcPushDownRule.java
