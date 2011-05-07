/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2011 Dynamo Business Intelligence Corporation

// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by Dynamo Business Intelligence Corporation.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

package net.sf.firewater;

import java.util.*;

import net.sf.farrago.fennel.rel.*;
import org.luciddb.lcs.*;
import org.eigenbase.reltype.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.jdbc.*;
import net.sf.farrago.query.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

import java.util.logging.*;
import net.sf.farrago.trace.*;

/**
 * FirewaterPushDistinctRule removes a top-level LhxAggRel step on the single
 * Firewater instance so that only each partition must do a DISTINCT check.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public class FirewaterPushDistinctRule extends RelOptRule {

    private static final Logger tracer
        = FarragoTrace.getClassTracer(FirewaterPushDistinctRule.class);

    public static final FirewaterPushDistinctRule instance =
        new FirewaterPushDistinctRule();

    /**
     * Creates a FirewaterPushDistinctRule
     */
    private FirewaterPushDistinctRule()
    {
        super(new RelOptRuleOperand(AggregateRel.class, ANY));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel agg = (AggregateRel) call.rels[0];
        if (agg.getGroupCount() < 1)
            return;

        tracer.info("ONMATCH CALLED\n\n");
        String count_key = agg.getChild().getRowType().getFields()[0].getName();

        boolean is_dm_key = false;
        // TODO: figure out how to get dm key from node, either by
        // getting a column set, server, or table props, and comparing
        // it to count_key
        // commented code below to help me remember later.
        /*MedJdbcQueryRel otherRel =
            (MedJdbcQueryRel) call.rels[2];
        MedJdbcColumnSet columnSet = otherRel.getColumnSet();
        // here we'd get columnSet.dm_key;
        MedJdbcDataServer server = columnSet.getDirectory().getServer();
        FarragoRepos repos =
            FarragoRelUtil.getPreparingStmt(tableRel).getRepos();
            */
        // this needs a femlocaltable
        //Properties tableProps =
        //    FarragoCatalogUtil.getStorageOptionsAsProperties(repos, (FemLocalTable)table);

        if (!is_dm_key)
            return;

        /*
        for (AggregateCall aggCall : agg.getAggCallList()) {
            tracer.info("AGG LIST\n\n");
            tracer.info(aggCall.toString());
            tracer.info(aggCall.getName());
            for (Integer i : aggCall.getArgList())
                tracer.info(i.toString() + ",");
            RelDataType r = aggCall.getType();
            tracer.info(":\n");
            tracer.info(r.toString());
            for (RelDataTypeField f : r.getFieldList())
                tracer.info(f.getName() + ";" + f.getIndex() + ";");
        }
        */
        tracer.info("Done, converting");

        RelNode fennelInput = mergeTraitsAndConvert(agg.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                agg.getChild());
        if (fennelInput == null)
            return;

        FennelAggRel fenn = new FennelAggRel(
                agg.getCluster(),
                fennelInput,
                agg.getGroupCount(),
                agg.getAggCallList());
        call.transformTo(fenn);
    }
}

