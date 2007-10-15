/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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

package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

/**
 * LcsSamplingRowScanRule converts a {@link SamplingRel} applied to 
 * an {@link LcsRowScanRel} into a {@link LcsSamplingRowScanRel}.
 *
 * @author Stephan Zuercher
 */
public class LcsSamplingRowScanRule
    extends RelOptRule
{
    public LcsSamplingRowScanRule()
    {
        super(
            new RelOptRuleOperand(
                SamplingRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                }));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }
    
    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SamplingRel origSamplingRel = (SamplingRel)call.rels[0];
        LcsRowScanRel origScanRel = (LcsRowScanRel)call.rels[1];
        
        Util.permAssert(origScanRel.isFullScan, "Cannot sample index scans");
        Util.permAssert(
            !origScanRel.hasResidualFilter, 
            "Cannot sample scans with residual filters");
        
        RelOptCluster cluster = origScanRel.getCluster();
        RelOptConnection connection = origScanRel.getConnection();
        RelNode[] origScanRelInputs = origScanRel.getInputs();
        LcsTable lcsTable = origScanRel.lcsTable;
        
        RelOptSamplingParameters samplingParams = 
            origSamplingRel.getSamplingParameters();

        RelNode [] inputs;
        if (samplingParams.isBernoulli()) {
            inputs = origScanRelInputs;
        } else {
            inputs = new RelNode[origScanRelInputs.length + 1];
            final int index = origScanRelInputs.length;
            
            IterOneRowRel oneRowRel = new IterOneRowRel(cluster);
            
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexProgramBuilder progBuilder = 
                new RexProgramBuilder(
                    oneRowRel.getRowType(), rexBuilder);
            
            SqlIdentifier callName = 
                new SqlIdentifier(
                    new String[] { "SYS_BOOT", "MGMT", "STAT_GET_ROW_COUNT" },
                    SqlParserPos.ZERO);
            
            FarragoUserDefinedRoutineLookup lookup = 
                ((FarragoPreparingStmt)connection).getRoutineLookup();
            
            List<SqlOperator> calls =
                lookup.lookupOperatorOverloads(
                    callName,
                    SqlFunctionCategory.UserDefinedFunction, 
                    SqlSyntax.Function);
            Util.permAssert(
                calls.size() == 1, 
                "couldn't find SYS_BOOT.MGMT.STAT_GET_ROW_COUNT");
            
            SqlOperator sqlCall = calls.get(0);
            
            String[] qualifiedTableName = lcsTable.getQualifiedName();
            RexNode callNode = 
                rexBuilder.makeCall(
                    sqlCall,
                    rexBuilder.makeLiteral(qualifiedTableName[0]),
                    rexBuilder.makeLiteral(qualifiedTableName[1]),
                    rexBuilder.makeLiteral(qualifiedTableName[2]));
            
            progBuilder.addProject(callNode, null);

            RexProgram prog = progBuilder.getProgram();
            
            IterCalcRel calcRel = 
                new IterCalcRel(
                    cluster, oneRowRel, prog, ProjectRelBase.Flags.None);
            
            inputs[index] = new IteratorToFennelConverter(cluster, calcRel);
        }

        
        LcsSamplingRowScanRel samplingScanRel =
            new LcsSamplingRowScanRel(
                cluster,
                inputs,
                lcsTable,
                origScanRel.clusteredIndexes,
                connection,
                origScanRel.projectedColumns,
                samplingParams);
        
        call.transformTo(samplingScanRel);
    }

}
