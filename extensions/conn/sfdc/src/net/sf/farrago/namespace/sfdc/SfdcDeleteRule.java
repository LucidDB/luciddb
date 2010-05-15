/*
// $Id$
// SFDC Connector is a SQL/MED connector for Salesforce.com for Farrago
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
package net.sf.farrago.namespace.sfdc;

import java.text.*;

import java.util.*;

import net.sf.farrago.namespace.sfdc.resource.*;
import net.sf.farrago.query.FarragoJavaUdxRel;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;


/**
 * SfdcDeleteRule is a rule to process the timestamp filter on SFDC deleted
 * objects
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcDeleteRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    String startTime;
    String endTime;
    boolean filterOnProj = false;

    // ~ Constructors ----------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SfdcDeleteRule object.
     */
    public SfdcDeleteRule(RelOptRuleOperand rule, String id)
    {
        super(rule, "SfdcDeleteRule: " + id);
        if (description.contains("filter on proj")) {
            filterOnProj = true;
        }
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        int relLength = call.rels.length;
        SfdcUdxRel udxRel = (SfdcUdxRel) call.rels[relLength - 1];

        if ((!udxRel.getUdx().getName().equalsIgnoreCase("sfdc_deleted"))
            && (!udxRel.getUdx().getName().equalsIgnoreCase("deleted")))
        {
            return;
        }

        RexBuilder rexBuilder = udxRel.getCluster().getRexBuilder();
        SargFactory sargFactory = new SargFactory(rexBuilder);
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

        FilterRel filter = null;

        if (filterOnProj) {
            filter = (FilterRel) call.rels[relLength - 3];
        } else {
            filter = (FilterRel) call.rels[relLength - 2];
        }

        RexNode filterExp = filter.getCondition();
        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(filterExp);

        if (sargBindingList.isEmpty()) {
            return;
        }

        boolean found = false;
        for (int i = 0; i < sargBindingList.size(); i++) {
            if (sargBindingList.get(i).getExpr().getDataType().toString()
                               .indexOf("TIMESTAMP")
                != -1)
            {
                if (!sargBindingList.get(i).getExpr().evaluate().isRange()) {
                    throw SfdcResourceObject.get().InvalidRangeException.ex();
                }
                SargIntervalSequence seq =
                    sargBindingList.get(i).getExpr().evaluate();
                List<SargInterval> sargIntList = seq.getList();

                SimpleDateFormat sdf =
                    new SimpleDateFormat(
                        "yyyy-MM-dd'T'HH:mm:ss");
                StringBuffer sbuf = null;
                try {
                    for (int j = 0; j < sargIntList.size(); j++) {
                        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        sbuf = new StringBuffer();
                        String start =
                            sdf.format(
                                ((Calendar)
                                    ((RexLiteral) sargIntList.get(j)
                                                             .getLowerBound()
                                                             .getCoordinate())
                                    .getValue()).getTime(),
                                sbuf,
                                new FieldPosition(0)).toString();
                        this.startTime = start;

                        sbuf = new StringBuffer();
                        String end =
                            sdf.format(
                                ((Calendar)
                                    ((RexLiteral) sargIntList.get(j)
                                                             .getUpperBound()
                                                             .getCoordinate())
                                    .getValue()).getTime(),
                                sbuf,
                                new FieldPosition(0)).toString();
                        this.endTime = end;
                    }
                } catch (Exception ex) {
                    throw SfdcResourceObject.get().InvalidRangeException.ex();
                }

                // remove timestamp
                found = true;
                sargBindingList.remove(i);
            } else {
                if ((i == (sargBindingList.size() - 1)) && !found) {
                    return;
                }
            }
        }

        RexNode [] udxRexNodes = udxRel.getChildExps();

        RexNode object =
            ((RexCall) (((RexCall) udxRexNodes[0]).getOperands())[0])
            .getOperands()[0];

        RexNode start = rexBuilder.makeLiteral(this.startTime);
        RexNode end = rexBuilder.makeLiteral(this.endTime);
        RexNode [] args2 = new RexNode[] { object, start, end };

        RexNode rexCall = rexBuilder.makeCall(udxRel.getUdx(), args2);

        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType resultType =
            typeFactory.createTypeWithNullability(
                udxRel.getRowType(),
                true);

        RelNode rel =
            new FarragoJavaUdxRel(
                udxRel.getCluster(),
                rexCall,
                resultType,
                udxRel.getServerMofId(),
                RelNode.emptyArray);

        rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

        RexNode extraFilter = null;
        List<SargBinding> residualSargBindingList = sargBindingList;

        RexNode residualRexNode =
            rexAnalyzer.getResidualSargRexNode(residualSargBindingList);

        RexNode postFilterRexNode = rexAnalyzer.getPostFilterRexNode();

        if ((residualRexNode != null) && (postFilterRexNode != null)) {
            extraFilter =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    residualRexNode,
                    postFilterRexNode);
        } else if (residualRexNode != null) {
            extraFilter = residualRexNode;
        } else if (postFilterRexNode != null) {
            extraFilter = postFilterRexNode;
        }

        if (extraFilter != null) {
            rel = new FilterRel(rel.getCluster(), rel, extraFilter);
        }

        if (relLength == 3) {
            ProjectRel pRel = null;
            if (filterOnProj) {
                pRel = (ProjectRel) call.rels[1];
            } else {
                pRel = (ProjectRel) call.rels[0];
            }

            rel =
                new ProjectRel(
                    pRel.getCluster(),
                    rel,
                    pRel.getProjectExps(),
                    pRel.getRowType(),
                    pRel.getFlags(),
                    pRel.getCollationList());
        }

        call.transformTo(rel);
        return;
    }

    // ~ Private Methods -------------------------------------------------------

}

// End SfdcDeleteRule.java
