/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2005 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * FtrsIndexJoinRule is a rule for converting a JoinRel into a
 * FtrsIndexSearchRel when the inputs have the appropriate form.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public FtrsIndexJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(FtrsIndexScanRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        RelNode leftRel = call.rels[1];
        FtrsIndexScanRel scanRel = (FtrsIndexScanRel) call.rels[2];

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        switch (joinRel.getJoinType()) {
        case JoinRel.JoinType.INNER:
        case JoinRel.JoinType.LEFT:
            break;
        default:
            return;
        }

        // TODO:  share more code with FtrsScanToSearchRule, and expand
        // set of supported join conditions
        if (scanRel.isOrderPreserving) {
            // index join is guaranteed to destroy scan ordering
            return;
        }

        FarragoRepos repos = FennelRelUtil.getRepos(scanRel);
        int [] joinFieldOrdinals = new int[2];
        if (!RelOptUtil.analyzeSimpleEquiJoin(joinRel, joinFieldOrdinals)) {
            return;
        }
        int leftOrdinal = joinFieldOrdinals[0];
        int rightOrdinal = joinFieldOrdinals[1];

        CwmColumn indexColumn = scanRel.getColumnForFieldAccess(rightOrdinal);
        assert (indexColumn != null);

        if (!scanRel.index.isClustered()) {
            // TODO:  support direct join against an unclustered index scan
            return;
        }

        // if we're working with a clustered index scan, consider all of
        // the unclustered indexes as well
        Iterator iter = FarragoCatalogUtil.getTableIndexes(
            repos, scanRel.ftrsTable.getCwmColumnSet()).iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();
            considerIndex(joinRel, index, scanRel, indexColumn, leftOrdinal,
                rightOrdinal, leftRel, call);
        }
    }

    private void considerIndex(
        JoinRel joinRel,
        FemLocalIndex index,
        FtrsIndexScanRel scanRel,
        CwmColumn indexColumn,
        int leftOrdinal,
        int rightOrdinal,
        RelNode leftRel,
        RelOptRuleCall call)
    {
        // TODO:  support compound keys
        boolean isUnique =
            index.isUnique() && (index.getIndexedFeature().size() == 1);

        boolean isOuter = (joinRel.getJoinType() == JoinRel.JoinType.LEFT);

        if (!FtrsScanToSearchRule.testIndexColumn(index, indexColumn)) {
            return;
        }

        // tell the index search how to project the key from its input
        Integer [] inputKeyProj = new Integer [] { new Integer(leftOrdinal) };

        RelDataTypeField [] leftFields = leftRel.getRowType().getFields();
        RelDataType leftType = leftFields[leftOrdinal].getType();
        RelDataType rightType =
            scanRel.getRowType().getFields()[rightOrdinal].getType();

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(scanRel);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();

        // decide what to do with nulls
        RelNode nullFilterRel;
        if (isOuter) {
            // can't filter out nulls when isOuter; instead, let Fennel
            // handle the null semantics
            nullFilterRel = leftRel;
            rightType =
                typeFactory.createTypeWithNullability(
                    rightType,
                    leftType.isNullable());
        } else {
            // filter out null search keys, since they never match
            nullFilterRel = RelOptUtil.createNullFilter(leftRel, inputKeyProj);
        }

        // cast the search keys from the left to the type of the search column
        // on the right
        RelNode castRel;
        int leftFieldCount = leftRel.getRowType().getFieldList().size();
        if (leftType.equals(rightType)) {
            // no cast required
            castRel = nullFilterRel;
        } else {
            RelDataType rightStructType =
                typeFactory.createStructType(
                    new RelDataType [] { rightType },
                    new String [] { "rightColumn" });

            RelDataType castRowType =
                typeFactory.createJoinType(
                    new RelDataType [] {
                        leftRel.getRowType(), rightStructType
                    });
            RexNode [] castExps = new RexNode[leftFieldCount + 1];
            String [] fieldNames = new String[leftFieldCount + 1];
            RexBuilder rexBuilder = leftRel.getCluster().rexBuilder;
            for (int i = 0; i < leftFieldCount; ++i) {
                castExps[i] =
                    rexBuilder.makeInputRef(
                        leftFields[i].getType(),
                        i);
                fieldNames[i] = leftFields[i].getName();
            }
            castExps[leftFieldCount] =
                rexBuilder.makeCast(rightType, castExps[leftOrdinal]);
            castRel =
                new ProjectRel(
                    nullFilterRel.getCluster(),
                    nullFilterRel,
                    castExps,
                    fieldNames,
                    ProjectRel.Flags.Boxed);

            // key now comes from extra cast field instead
            inputKeyProj = new Integer [] { new Integer(leftFieldCount) };
        }

        RelNode fennelInput =
            convert(castRel, FennelPullRel.FENNEL_PULL_CONVENTION);

        // tell the index search to propagate everything from its input as join
        // fields
        Integer [] inputJoinProj =
            FennelRelUtil.newIotaProjection(leftFieldCount);

        if (!index.isClustered() && scanRel.index.isClustered()) {
            Integer [] clusteredKeyColumns =
                scanRel.ftrsTable.getIndexGuide().getClusteredDistinctKeyArray(
                    scanRel.index);

            // REVIEW:  in many cases it would probably be more efficient to
            // hide the unclustered-to-clustered translation inside a special
            // TupleStream, otherwise the left-hand join fields get
            // propagated one extra time.
            FtrsIndexScanRel unclusteredScan =
                new FtrsIndexScanRel(
                    scanRel.getCluster(),
                    scanRel.ftrsTable,
                    index,
                    scanRel.getConnection(),
                    clusteredKeyColumns,
                    scanRel.isOrderPreserving);
            FtrsIndexSearchRel unclusteredSearch =
                new FtrsIndexSearchRel(unclusteredScan, fennelInput, isUnique,
                    isOuter, inputKeyProj, inputJoinProj);

            // tell the search against the clustered index where to find the
            // keys in the output of the unclustered index search, and what to
            // propagate (everything BUT the clustered index key which was
            // tacked onto the end)
            Integer [] clusteredInputKeyProj =
                FennelRelUtil.newBiasedIotaProjection(clusteredKeyColumns.length,
                    inputJoinProj.length);

            FtrsIndexSearchRel clusteredSearch =
                new FtrsIndexSearchRel(scanRel, unclusteredSearch, true,
                    isOuter, clusteredInputKeyProj, inputJoinProj);

            call.transformTo(clusteredSearch);
        } else {
            FtrsIndexSearchRel searchRel =
                new FtrsIndexSearchRel(scanRel, fennelInput, isUnique,
                    isOuter, inputKeyProj, inputJoinProj);
            call.transformTo(searchRel);
        }
    }
}


// End FtrsIndexJoinRule.java
