/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

import java.util.*;
import java.util.List;

import openjava.ptree.Literal;


/**
 * FtrsIndexScanRel is the relational expression corresponding to a scan via
 * a particular index over the contents of a table stored in FTRS format.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexScanRel extends TableAccessRel implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    /** Index to use for access. */
    final CwmSqlindex index;

    /** Refinement for super.table. */
    final FtrsTable ftrsTable;

    /**
     * Array of 0-based column ordinals to project; if null, project all
     * columns.  Note that these ordinals are relative to the table, not the
     * index.
     */
    final Integer [] projectedColumns;

    final boolean isOrderPreserving;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsIndexScanRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param ftrsTable table being scanned
     * @param index index to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     * @param isOrderPreserving if true, returned rows must be in index order;
     * if false, rows can be returned out of order
     */
    public FtrsIndexScanRel(
        VolcanoCluster cluster,
        FtrsTable ftrsTable,
        CwmSqlindex index,
        SaffronConnection connection,
        Integer [] projectedColumns,
        boolean isOrderPreserving)
    {
        super(cluster,ftrsTable,connection);
        this.ftrsTable = ftrsTable;
        this.index = index;
        this.projectedColumns = projectedColumns;
        this.isOrderPreserving = isOrderPreserving;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Get the column referenced by a FieldAccess relative to this scan.
     *
     * @param columnOrdinal 0-based ordinal of an output field of the scan
     *
     * @return underlying column
     */
    public CwmColumn getColumnForFieldAccess(int columnOrdinal)
    {
        assert columnOrdinal >= 0;
        if (projectedColumns != null) {
            columnOrdinal = projectedColumns[columnOrdinal].intValue();
        }
        return (CwmColumn) ftrsTable.getCwmColumnSet().getFeature().get(
            columnOrdinal);
    }

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement FennelRel
    public FarragoPreparingStmt getPreparingStmt()
    {
        return ftrsTable.getPreparingStmt();
    }

    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        return computeCost(planner,getRows());
    }

    // implement SaffronRel
    public SaffronType deriveRowType()
    {
        if (projectedColumns == null) {
            return super.deriveRowType();
        } else {
            final SaffronField [] fields = table.getRowType().getFields();
            return cluster.typeFactory.createProjectType(
                new SaffronTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return projectedColumns.length;
                    }

                    public String getFieldName(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getName();
                    }

                    public SaffronType getFieldType(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getType();
                    }
                });
        }
    }

    // override TableAccess
    public void explain(PlanWriter pw)
    {
        Object projection;
        if (projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(projectedColumns);
        }
        pw.explain(
            this,
            new String [] {
                "table","projection","index","preserveOrder" },
            new Object [] {
                Arrays.asList(ftrsTable.getQualifiedName()),
                projection,index.getName(),
                Boolean.valueOf(isOrderPreserving)
            });
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoCatalog catalog = getPreparingStmt().getCatalog();

        FemIndexScanDef scanStream =
            catalog.newFemIndexScanDef();

        defineScanStream(scanStream);

        return scanStream;
    }

    // implement SaffronRel
    PlanCost computeCost(SaffronPlanner planner,double dRows)
    {
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldCount();

        FarragoCatalog catalog = getPreparingStmt().getCatalog();
        int nIndexCols =
            catalog.isClustered(index)
            ? ftrsTable.getCwmColumnSet().getFeature().size()
            : FtrsUtil.getUnclusteredCoverageColList(catalog,index).size();

        double dIo = dRows * nIndexCols;

        return planner.makeCost(dRows,dCpu,dIo);
    }

    /**
     * Fill in a stream definition for this scan.
     *
     * @param scanStream stream definition to fill in
     */
    void defineScanStream(FemIndexScanDef scanStream)
    {
        FarragoCatalog catalog = getPreparingStmt().getCatalog();

        if (!catalog.isTemporary(index)) {
            scanStream.setRootPageId(
                getPreparingStmt().getIndexMap().getIndexRoot(index));
        } else {
            // For a temporary index, each execution needs to bind to
            // a session-private root.  So don't burn anything into
            // the plan.
            scanStream.setRootPageId(-1);
        }
        scanStream.setSegmentId(
            FtrsDataServer.getIndexSegmentId(index));
        scanStream.setIndexId(
            JmiUtil.getObjectId(index));

        scanStream.setTupleDesc(
            FtrsUtil.getCoverageTupleDescriptor(
                (FarragoTypeFactory) cluster.typeFactory,
                index));

        scanStream.setKeyProj(
            FtrsUtil.getDistinctKeyProjection(catalog,index));

        Integer [] projection = computeProjectedColumns();
        Integer [] indexProjection;

        if (catalog.isClustered(index)) {
            indexProjection = projection;
        } else {
            // transform from table-relative to index-relative ordinals
            indexProjection = new Integer[projection.length];
            List indexTableColList =
                Arrays.asList(
                    FtrsUtil.getUnclusteredCoverageArray(catalog,index));
            for (int i = 0; i < projection.length; ++i) {
                Integer iTableCol = projection[i];
                int iIndexCol = indexTableColList.indexOf(iTableCol);
                assert (iIndexCol != -1);
                indexProjection[i] = new Integer(iIndexCol);
            }
        }

        scanStream.setOutputProj(
            FennelRelUtil.createTupleProjection(catalog,indexProjection));
    }

    private Integer [] computeProjectedColumns()
    {
        if (projectedColumns != null) {
            return projectedColumns;
        }
        int n = table.getRowType().getFieldCount();
        return FennelRelUtil.newIotaProjection(n);
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        Integer [] indexedCols = FtrsUtil.getCollationKeyArray(
            getPreparingStmt().getCatalog(),
            index);
        List collationList = new ArrayList();
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
        return (RelFieldCollation []) collationList.toArray(
            RelFieldCollation.emptyCollationArray);
    }
}


// End FtrsIndexScanRel.java
