/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


// REVIEW jvs 3-Dec-2006: Need to implement getChildExps() like SortRel?
// Should probably factor out a SortRelBase.

/**
 * FennelSortRel is the relational expression corresponding to a sort
 * implemented inside of Fennel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelSortRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RelFieldCollation [] collations;

    /**
     * Whether to discard tuples with duplicate keys.
     */
    private final boolean discardDuplicates;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelSortRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be sorted
     * @param keyProjection 0-based ordinals of fields making up sort key (all
     * ascending)
     * @param discardDuplicates whether to discard duplicates based on key
     */
    public FennelSortRel(
        RelOptCluster cluster,
        RelNode child,
        Integer [] keyProjection,
        boolean discardDuplicates)
    {
        this(
            cluster,
            child,
            convertKeyProjection(keyProjection),
            discardDuplicates);
    }

    /**
     * Creates a new FennelSortRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be sorted
     * @param collations array of sort specifications
     * @param discardDuplicates whether to discard duplicates based on key
     */
    public FennelSortRel(
        RelOptCluster cluster,
        RelNode child,
        RelFieldCollation [] collations,
        boolean discardDuplicates)
    {
        super(cluster, child);

        // TODO:  validate that collations are distinct
        this.collations = collations;
        this.discardDuplicates = discardDuplicates;
    }

    //~ Methods ----------------------------------------------------------------

    private static RelFieldCollation [] convertKeyProjection(
        Integer [] keyProjection)
    {
        RelFieldCollation [] collations =
            new RelFieldCollation[keyProjection.length];
        for (int i = 0; i < keyProjection.length; ++i) {
            collations[i] =
                new RelFieldCollation(
                    keyProjection[i],
                    RelFieldCollation.Direction.Ascending);
        }
        return collations;
    }

    // override Rel
    public boolean isDistinct()
    {
        // sort results are distinct if duplicates are discarded AND
        // the sort key is the whole tuple
        return discardDuplicates
            && (collations.length == getRowType().getFieldList().size());
    }

    public boolean isDiscardDuplicates()
    {
        return discardDuplicates;
    }

    // implement Cloneable
    public FennelSortRel clone()
    {
        FennelSortRel clone =
            new FennelSortRel(
                getCluster(),
                getChild().clone(),
                collations,
                discardDuplicates);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public double getRows()
    {
        double rowCount = RelMetadataQuery.getRowCount(getChild());
        if (discardDuplicates) {
            // Assume that each sort column has 50% of the value count.
            // Therefore one sort column has .5 * rowCount,
            // 2 sort columns give .75 * rowCount.
            // Zero sort columns yields 1 row (or 0 if the input is empty).
            if (collations.length == 0) {
                rowCount = 1;
            } else {
                rowCount *= (1.0 - Math.pow(.5, collations.length));
            }
        }
        return rowCount;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing
        double rowCount = RelMetadataQuery.getRowCount(this);
        double bytesPerRow = 1;
        return planner.makeCost(
            rowCount,
            Util.nLogN(rowCount),
            rowCount * bytesPerRow);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        // TODO jvs 3-Dec-2006:  fix this and SortRel to be consistent

        String [] keys = new String[collations.length];
        for (int i = 0; i < collations.length; ++i) {
            keys[i] = "" + collations[i].getFieldIndex();
            if (collations[i].getDirection()
                != RelFieldCollation.Direction.Ascending)
            {
                keys[i] += " " + collations[i].getDirection();
            }
        }

        pw.explain(
            this,
            new String[] { "child", "key", "discardDuplicates" },
            new Object[] {
                Arrays.asList(keys),
                Boolean.valueOf(discardDuplicates)
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemSortingStreamDef sortingStream = repos.newFemSortingStreamDef();

        sortingStream.setDistinctness(
            discardDuplicates ? DistinctnessEnum.DUP_DISCARD
            : DistinctnessEnum.DUP_ALLOW);
        List<Integer> keyProj = new ArrayList<Integer>();
        List<Integer> descendingProj = new ArrayList<Integer>();
        int iKey = 0;
        for (RelFieldCollation collation : collations) {
            keyProj.add(collation.getFieldIndex());
            if (collation.getDirection()
                != RelFieldCollation.Direction.Ascending)
            {
                assert (collation.getDirection()
                    == RelFieldCollation.Direction.Descending);
                descendingProj.add(iKey);
            }
            ++iKey;
        }
        sortingStream.setKeyProj(
            FennelRelUtil.createTupleProjection(
                repos,
                keyProj));
        sortingStream.setDescendingProj(
            FennelRelUtil.createTupleProjection(
                repos,
                descendingProj));
        Double numInputRows = RelMetadataQuery.getRowCount(getChild());
        if (numInputRows == null) {
            sortingStream.setEstimatedNumRows(-1);
        } else {
            sortingStream.setEstimatedNumRows(numInputRows.longValue());
        }
        sortingStream.setEarlyClose(false);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            sortingStream);
        return sortingStream;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        return collations;
    }
}

// End FennelSortRel.java
