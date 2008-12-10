/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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
package net.sf.farrago.namespace.ftrs;

import java.util.*;
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;

import openjava.ptree.Literal;

import org.eigenbase.jmi.JmiObjUtil;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FtrsIndexScanRel is the relational expression corresponding to a scan via a
 * particular index over the contents of a table stored in FTRS format.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexScanRel
    extends TableAccessRelBase
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Index to use for access.
     */
    final FemLocalIndex index;

    /**
     * Refinement for super.table.
     */
    final FtrsTable ftrsTable;

    /**
     * Array of 0-based flattened column ordinals to project; if null, project
     * all columns. Note that these ordinals are relative to the table, not the
     * index.
     */
    final Integer [] projectedColumns;
    final boolean isOrderPreserving;

    private FtrsIndexGuide indexGuide;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FtrsIndexScanRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param ftrsTable table being scanned
     * @param index index to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     * @param isOrderPreserving if true, returned rows must be in index order;
     * if false, rows can be returned out of order
     */
    public FtrsIndexScanRel(
        RelOptCluster cluster,
        FtrsTable ftrsTable,
        FemLocalIndex index,
        RelOptConnection connection,
        Integer [] projectedColumns,
        boolean isOrderPreserving)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            ftrsTable,
            connection);
        this.ftrsTable = ftrsTable;
        this.index = index;
        this.projectedColumns = projectedColumns;
        this.isOrderPreserving = isOrderPreserving;
        assert ftrsTable.getPreparingStmt()
            == FennelRelUtil.getPreparingStmt(this);
    }

    //~ Methods ----------------------------------------------------------------

    public FtrsIndexGuide getIndexGuide()
    {
        if (indexGuide == null) {
            indexGuide =
                new FtrsIndexGuide(
                    ftrsTable.getPreparingStmt().getFarragoTypeFactory(),
                    ftrsTable.getCwmColumnSet());
        }
        return indexGuide;
    }

    /**
     * Gets the column referenced by a FieldAccess relative to this scan.
     *
     * @param fieldOrdinal 0-based ordinal of an output field of the scan
     *
     * @return underlying column
     */
    public FemAbstractColumn getColumnForFieldAccess(int fieldOrdinal)
    {
        assert fieldOrdinal >= 0;
        if (projectedColumns != null) {
            fieldOrdinal = projectedColumns[fieldOrdinal].intValue();
        }
        int columnOrdinal = getIndexGuide().unFlattenOrdinal(fieldOrdinal);
        return (FemAbstractColumn) ftrsTable.getCwmColumnSet().getFeature().get(
            columnOrdinal);
    }

    // implement RelNode
    public FtrsIndexScanRel clone()
    {
        FtrsIndexScanRel clone =
            new FtrsIndexScanRel(
                getCluster(),
                ftrsTable,
                index,
                connection,
                projectedColumns,
                isOrderPreserving);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        RelOptCost cost =
            computeCost(
                planner,
                RelMetadataQuery.getRowCount(this));

        // NOTE jvs 24-Jan-2006:  This is just here to fudge the optimizer
        // into using index scans where possible.
        cost = cost.multiplyBy(10);

        return cost;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        RelDataType flattenedRowType =
            ftrsTable.getIndexGuide().getFlattenedRowType();
        if (projectedColumns == null) {
            return flattenedRowType;
        } else {
            final RelDataTypeField [] fields = flattenedRowType.getFields();
            return getCluster().getTypeFactory().createStructType(
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return projectedColumns.length;
                    }

                    public String getFieldName(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getName();
                    }

                    public RelDataType getFieldType(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getType();
                    }
                });
        }
    }

    // override TableAccess
    public void explain(RelOptPlanWriter pw)
    {
        Object projection;
        if (projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(projectedColumns);
        }
        pw.explain(
            this,
            new String[] { "table", "projection", "index", "preserveOrder" },
            new Object[] {
                Arrays.asList(ftrsTable.getQualifiedName()), projection,
                index.getName(), Boolean.valueOf(isOrderPreserving)
            });
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemIndexScanDef scanStream = repos.newFemIndexScanDef();

        defineScanStream(scanStream);

        return scanStream;
    }

    // implement RelNode
    RelOptCost computeCost(
        RelOptPlanner planner,
        double dRows)
    {
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();

        FtrsIndexGuide indexGuide = ftrsTable.getIndexGuide();
        int nIndexCols =
            index.isClustered()
            ? ftrsTable.getIndexGuide().getFlattenedRowType().getFieldList()
            .size()
            : indexGuide.getUnclusteredCoverageColList(index).size();

        double dIo = dRows * nIndexCols;

        return planner.makeCost(dRows, dCpu, dIo);
    }

    /**
     * Fills in a stream definition for this scan.
     *
     * @param scanStream stream definition to fill in
     */
    void defineScanStream(FemIndexScanDef scanStream)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);

        if (!FarragoCatalogUtil.isIndexTemporary(index)) {
            scanStream.setRootPageId(
                stmt.getIndexMap().getIndexRoot(index));
        } else {
            // For a temporary index, each execution needs to bind to
            // a session-private root.  So don't burn anything into
            // the plan.
            scanStream.setRootPageId(-1);
        }
        scanStream.setReadOnlyCommittedData(false);
        scanStream.setRootPageIdParamId(0);
        scanStream.setSegmentId(FtrsDataServer.getIndexSegmentId(index));
        scanStream.setIndexId(JmiObjUtil.getObjectId(index));

        FtrsIndexGuide indexGuide = ftrsTable.getIndexGuide();

        CwmTable oldTable = stmt.getIndexMap().getOldTableStructure();
        if (oldTable != null) {
            // REVIEW jvs 4-Dec-2008:  this wouldn't work for
            // an unclustered index scan, so if somehow we ever
            // ended up choosing one of those for ALTER TABLE
            // ADD COLUMN, I think we'd be in trouble.
            indexGuide = new FtrsIndexGuide(
                stmt.getFarragoTypeFactory(),
                oldTable);
        }
        scanStream.setTupleDesc(
            indexGuide.getCoverageTupleDescriptor(index));

        scanStream.setKeyProj(
            indexGuide.getDistinctKeyProjection(index));

        Integer [] projection = computeProjectedColumns();
        Integer [] indexProjection;

        if (index.isClustered()) {
            indexProjection = projection;
        } else {
            // transform from table-relative to index-relative ordinals
            indexProjection = new Integer[projection.length];
            List<Integer> indexTableColList =
                Arrays.asList(
                    indexGuide.getUnclusteredCoverageArray(index));
            for (int i = 0; i < projection.length; ++i) {
                Integer iTableCol = projection[i];
                int iIndexCol = indexTableColList.indexOf(iTableCol);
                assert (iIndexCol != -1);
                indexProjection[i] = iIndexCol;
            }
        }

        scanStream.setOutputProj(
            FennelRelUtil.createTupleProjection(repos, indexProjection));
    }

    private Integer [] computeProjectedColumns()
    {
        if (projectedColumns != null) {
            return projectedColumns;
        }
        int n =
            ftrsTable.getIndexGuide().getFlattenedRowType().getFieldList()
            .size();
        return FennelRelUtil.newIotaProjection(n);
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        Integer [] indexedCols =
            ftrsTable.getIndexGuide().getCollationKeyArray(index);
        List<RelFieldCollation> collationList =
            new ArrayList<RelFieldCollation>();
        for (int i = 0; i < indexedCols.length; ++i) {
            int iCol = indexedCols[i].intValue();
            RelFieldCollation collation = null;
            if (projectedColumns == null) {
                collation = new RelFieldCollation(iCol);
            } else {
                for (int j = 0; j < projectedColumns.length; ++j) {
                    if (projectedColumns[j].intValue() == iCol) {
                        collation = new RelFieldCollation(j);
                        break;
                    }
                }
                if (collation == null) {
                    break;
                }
            }
            collationList.add(collation);
        }
        return collationList.toArray(
            new RelFieldCollation[collationList.size()]);
    }
}

// End FtrsIndexScanRel.java
