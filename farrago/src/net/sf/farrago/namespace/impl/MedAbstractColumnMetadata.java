/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.rel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.stat.*;
import org.eigenbase.util14.*;


/**
 * MedAbstractColumnMetadata is a base class that provides common logic for
 * implementing certain metadata queries that relate to columns. Other classes
 * should derive from this class to provide implementations specific to
 * different data wrappers.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public abstract class MedAbstractColumnMetadata
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @deprecated
     */
    public Set<BitSet> getUniqueKeys(
        RelNode rel,
        FarragoRepos repos)
    {
        return getUniqueKeys(rel, repos, false);
    }

    public Set<BitSet> getUniqueKeys(
        RelNode rel,
        FarragoRepos repos,
        boolean ignoreNulls)
    {
        // this method only handles table level relnodes
        if (rel.getTable() == null) {
            return null;
        }

        MedAbstractColumnSet table = (MedAbstractColumnSet) rel.getTable();
        if (table.getCwmColumnSet() == null) {
            return null;
        }

        Set<BitSet> retSet = new HashSet<BitSet>();

        // first retrieve the columns from the primary key
        FemPrimaryKeyConstraint primKey =
            FarragoCatalogUtil.getPrimaryKey(table.getCwmColumnSet());
        if (primKey != null) {
            addKeyCols(
                rel,
                repos,
                (List) primKey.getFeature(),
                false,
                retSet);
        }

        // then, loop through each unique constraint, looking for unique
        // constraints where all columns in the constraint are non-null
        List<FemUniqueKeyConstraint> uniqueConstraints =
            FarragoCatalogUtil.getUniqueKeyConstraints(
                table.getCwmColumnSet());
        for (FemUniqueKeyConstraint uniqueConstraint : uniqueConstraints) {
            addKeyCols(
                rel,
                repos,
                (List) uniqueConstraint.getFeature(),
                !ignoreNulls,
                retSet);
        }

        return retSet;
    }

    /**
     * Forms bitmaps representing the columns in a constraint and adds them to a
     * set
     *
     * @param rel RelNode that the constraint belongs to
     * @param repos repository
     * @param keyCols list of columns that make up a constraint
     * @param checkNulls if true, don't add the columns of the constraint if the
     * columns allow nulls
     * @param keyList the set where the bitmaps will be added
     */
    private void addKeyCols(
        RelNode rel,
        FarragoRepos repos,
        List<FemAbstractColumn> keyCols,
        boolean checkNulls,
        Set<BitSet> keyList)
    {
        BitSet colMask = new BitSet();
        for (FemAbstractColumn keyCol : keyCols) {
            if (checkNulls
                && FarragoCatalogUtil.isColumnNullable(repos, keyCol))
            {
                return;
            }
            int fieldNo = mapColumnToField(rel, keyCol);
            if (fieldNo == -1) {
                return;
            }
            colMask.set(fieldNo);
        }
        keyList.add(colMask);
    }

    /**
     * Maps a FemAbstractColumn to its corresponding field reference in the
     * RelNode.
     *
     * @param rel RelNode corresponding to the column
     * @param keyCol the column whose field ordinal will be returned
     *
     * @return field ordinal relative to the RelNode; -1 if the column is not
     * accessed by the RelNode
     */
    protected abstract int mapColumnToField(
        RelNode rel,
        FemAbstractColumn keyCol);

    /**
     * @deprecated
     */
    public Boolean areColumnsUnique(
        RelNode rel,
        BitSet columns,
        FarragoRepos repos)
    {
        return areColumnsUnique(rel, columns, repos, false);
    }

    public Boolean areColumnsUnique(
        RelNode rel,
        BitSet columns,
        FarragoRepos repos,
        boolean ignoreNulls)
    {
        Set<BitSet> uniqueColSets = getUniqueKeys(rel, repos, ignoreNulls);
        return areColumnsUniqueForKeys(uniqueColSets, columns);
    }

    public static Boolean areColumnsUniqueForKeys(
        Set<BitSet> uniqueColSets,
        BitSet columns)
    {
        if (uniqueColSets == null) {
            return null;
        }
        for (BitSet colSet : uniqueColSets) {
            if (RelOptUtil.contains(columns, colSet)) {
                return true;
            }
        }
        return false;
    }

    public Double getPopulationSize(RelNode rel, BitSet groupKey)
    {
        // this method only handles table level relnodes
        if (rel.getTable() == null) {
            return null;
        }

        double population = 1.0;

        // if columns are part of a unique key, then just return the rowcount
        if (RelMdUtil.areColumnsDefinitelyUnique(rel, groupKey)) {
            return RelMetadataQuery.getRowCount(rel);
        }

        // if no stats are available, return null
        RelStatSource tabStats = RelMetadataQuery.getStatistics(rel);
        if (tabStats == null) {
            return null;
        }

        // multiply by the cardinality of each column
        for (
            int col = groupKey.nextSetBit(0);
            col >= 0;
            col = groupKey.nextSetBit(col + 1))
        {
            // calculate the original ordinal (before projection)
            int origCol = mapFieldToColumnOrdinal(rel, col);
            if (origCol == -1) {
                return null;
            }

            RelStatColumnStatistics colStats =
                tabStats.getColumnStatistics(origCol, null);
            if (colStats == null) {
                return null;
            }
            Double colCard = colStats.getCardinality();
            if (colCard == null) {
                return null;
            }
            population *= colCard;
        }

        // cap the number of distinct values
        return RelMdUtil.numDistinctVals(
            population,
            RelMetadataQuery.getRowCount(rel));
    }

    /**
     * Maps a field reference to the underlying column ordinal corresponding to
     * the FemAbstractColumn representing the column.
     *
     * @param rel RelNode corresponding to the column
     * @param fieldNo the ordinal of the field reference
     *
     * @return column ordinal of the underlying FemAbstractColumn; -1 if the
     * column does not map to actual FemAbstractColumn
     */
    protected abstract int mapFieldToColumnOrdinal(RelNode rel, int fieldNo);

    public Double getDistinctRowCount(
        RelNode rel,
        BitSet groupKey,
        RexNode predicate)
    {
        // this method only handles table level relnodes
        if (rel.getTable() == null) {
            return null;
        }

        // if the columns form a unique key or are part of a unique key,
        // then just return the rowcount times the selectivity of the
        // predicate
        boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(rel, groupKey);
        if (uniq) {
            return NumberUtil.multiply(
                RelMetadataQuery.getRowCount(rel),
                RelMetadataQuery.getSelectivity(rel, predicate));
        }

        // if no stats are available, return null
        RelStatSource tabStats = RelMetadataQuery.getStatistics(rel);
        if (tabStats == null) {
            return null;
        }

        Map<CwmColumn, SargIntervalSequence> col2SeqMap = null;
        RexNode nonSargFilters = null;
        if (predicate != null) {
            SargFactory sargFactory =
                new SargFactory(rel.getCluster().getRexBuilder());
            SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

            // determine which predicates are sargable and which aren't
            List<SargBinding> sargBindingList =
                rexAnalyzer.analyzeAll(predicate);
            nonSargFilters = rexAnalyzer.getNonSargFilterRexNode();

            if (!sargBindingList.isEmpty()) {
                col2SeqMap = new HashMap<CwmColumn, SargIntervalSequence>();

                for (int i = 0; i < sargBindingList.size(); i++) {
                    SargBinding sargBinding = sargBindingList.get(i);
                    RexInputRef fieldAccess = sargBinding.getInputRef();
                    FemAbstractColumn filterColumn =
                        mapFieldToColumn(
                            rel,
                            fieldAccess.getIndex());
                    if (filterColumn != null) {
                        SargIntervalSequence sargSeq =
                            FennelRelUtil.evaluateSargExpr(
                                sargBinding.getExpr());

                        col2SeqMap.put(filterColumn, sargSeq);
                    }
                }
            }
        }

        // loop through each column and determine the cardinality of the
        // column
        Double distRowCount = 1.0;
        for (
            int fieldNo = groupKey.nextSetBit(0);
            fieldNo >= 0;
            fieldNo = groupKey.nextSetBit(fieldNo + 1))
        {
            // if the column has sargable predicates, compute the
            // cardinality based on the predicates; otherwise, just compute
            // the full cardinality of the column
            RelStatColumnStatistics colStats = null;

            FemAbstractColumn col = mapFieldToColumn(rel, fieldNo);
            if (col == null) {
                return null;
            }
            int origColno = mapFieldToColumnOrdinal(rel, fieldNo);

            if (col2SeqMap != null) {
                SargIntervalSequence sargSeq = col2SeqMap.get(col);

                // getColumnStatistics uses original field position
                colStats = tabStats.getColumnStatistics(origColno, sargSeq);
            } else {
                // getColumnStatistics uses original field position
                colStats = tabStats.getColumnStatistics(origColno, null);
            }
            if (colStats == null) {
                return null;
            }
            Double colCard = colStats.getCardinality();
            if (colCard == null) {
                return null;
            }
            distRowCount = distRowCount * colCard;
        }

        // reduce cardinality by the selectivity of the non-sargable
        // predicates (which includes any semijoin filters)
        distRowCount *= RelMdUtil.guessSelectivity(nonSargFilters);

        // return value should be no higher than just applying the selectivity
        // of all predicates on the rel
        Double minRowCount =
            NumberUtil.multiply(
                RelMetadataQuery.getRowCount(rel),
                RelMetadataQuery.getSelectivity(rel, predicate));
        if (minRowCount != null) {
            distRowCount = Math.min(distRowCount, minRowCount);
        }

        return distRowCount;
    }

    /**
     * Maps a field reference to its underlying FemAbstractColumn
     *
     * @param rel RelNode corresponding to the field
     * @param fieldNo the ordinal of the field reference
     *
     * @return underlying FemAbstractColumn; null if no underlying
     * FemAbstractColumn
     */
    protected abstract FemAbstractColumn mapFieldToColumn(
        RelNode rel,
        int fieldNo);
}

// End MedAbstractColumnMetadata.java
