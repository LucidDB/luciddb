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
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

import java.util.*;

/**
 * FtrsTableModificationRel is the relational expression corresponding to
 * modification of a FTRS table.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableModificationRel extends TableModificationRel
    implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    /** Refinement for TableModificationRel.table. */
    final FtrsTable ftrsTable;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableModificationRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param ftrsTable target table
     * @param connection connection expression
     * @param child child producing rows to be inserted
     * @param operation modification operation to perform
     */
    public FtrsTableModificationRel(
        VolcanoCluster cluster,
        FtrsTable ftrsTable,
        SaffronConnection connection,
        SaffronRel child,
        Operation operation,
        List updateColumnList)
    {
        super(cluster,ftrsTable,connection,child,operation,updateColumnList);
        this.ftrsTable = ftrsTable;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FennelRel
    public SaffronConnection getConnection()
    {
        return connection;
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

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) cluster.typeFactory;
    }

    // implement Cloneable
    public Object clone()
    {
        return new FtrsTableModificationRel(
            cluster,
            ftrsTable,
            getConnection(),
            OptUtil.clone(child),
            getOperation(),
            getUpdateColumnList());
    }

    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        // TODO:  the real thing
        return planner.makeTinyCost();
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1);
        return implementor.implementChild(this,0,child);
    }

    private List getUpdateCwmColumnList()
    {
        int n = getUpdateColumnList().size();
        List list = new ArrayList();
        Collection columns = ftrsTable.getCwmColumnSet().getFeature();
        for (int i = 0; i < n; i++) {
            String columnName = (String) getUpdateColumnList().get(i);
            CwmColumn column = (CwmColumn)
                getCatalog().getModelElement(columns,columnName);
            list.add(column);
        }
        return list;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FarragoRelImplementor implementor)
    {
        FemExecutionStreamDef input = implementor.implementFennelRel(child);
        FarragoTypeFactory typeFactory = getFarragoTypeFactory();
        if (!(ftrsTable.getCwmColumnSet() instanceof CwmTable)) {
            // e.g. view update
            throw Util.needToImplement(ftrsTable.getCwmColumnSet());
        }
        CwmTable table = (CwmTable) ftrsTable.getCwmColumnSet();
        FarragoCatalog catalog = getCatalog();

        List updateCwmColumnList = null;
        
        FemTableWriterDef tableWriterDef;
        switch (getOperation().ordinal_) {
        case TableModificationRel.Operation.INSERT_ORDINAL:
            tableWriterDef = catalog.newFemTableInserterDef();
            break;
        case TableModificationRel.Operation.DELETE_ORDINAL:
            tableWriterDef = catalog.newFemTableDeleterDef();
            break;
        case TableModificationRel.Operation.UPDATE_ORDINAL:
            FemTableUpdaterDef tableUpdaterDef =
                catalog.newFemTableUpdaterDef();
            tableWriterDef = tableUpdaterDef;
            updateCwmColumnList = getUpdateCwmColumnList();
            tableUpdaterDef.setUpdateProj(
                FennelRelUtil.createTupleProjectionFromColumnList(
                    catalog,
                    updateCwmColumnList));
            break;
        default:
            throw getOperation().unexpected();
        }

        // This is to account for total number of pages needed to perform an
        // update on a single index.  Pages are only locked for the duration of
        // one index update, so they don't need to be charged per index (unless
        // we start parallelizing index updates).  TODO: determine the correct
        // number; 4 is just a guess.
        int nPagesMin = 4;

        // We need to be careful about the order in which we write to indexes.
        // This is required because secondary indexes rely on the clustering
        // key's uniqueness for discriminating entries, e.g. during rollback.
        // So when the clustering key is declared as unique, we should insert
        // into it first.  If a uniqueness violation is detected, we will abort
        // the insertion and the other indexes will be left untouched.
        // However, when the clustering key is not unique (except by virtue of
        // its appended primary key fields), then we should insert into the
        // primary key index first instead.  If that passes, the combination of
        // the clustering key with the primary key is guaranteed to be unique
        // also.  For updates, there is another consideration:  indexes which
        // are updated in place cannot be rolled back individually, so
        // they must come last (after any possible constraint violations).

        // REVIEW:  update/delete resources
        
        CwmSqlindex clusteredIndex = catalog.getClusteredIndex(table);
        boolean clusteredFirst = clusteredIndex.isUnique();

        List firstList = new ArrayList();
        List secondList = new ArrayList();

        Iterator indexIter =
            catalog.getIndexes(table).iterator();
        while (indexIter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) indexIter.next();

            boolean updateInPlace = false;

            if (updateCwmColumnList != null) {
                if (index != clusteredIndex) {
                    List coverageList =
                        FtrsUtil.getUnclusteredCoverageColList(
                            catalog,index);
                    if (!coverageList.removeAll(updateCwmColumnList)) {
                        // no intersection between update list and index
                        // coverage, so skip this index entirely
                        continue;
                    }
                }

                List distinctKeyList = FtrsUtil.getDistinctKeyColList(
                    catalog,index);
                if (!distinctKeyList.removeAll(updateCwmColumnList)) {
                    // distinct key is not being changed, so it's safe to
                    // attempt update-in-place
                    updateInPlace = true;
                }
            }
            
            FemIndexWriterDef indexWriter =
                catalog.fennelPackage.getFemIndexWriterDef()
                                         .createFemIndexWriterDef();
            if (!catalog.isTemporary(index)) {
                indexWriter.setRootPageId(
                    getPreparingStmt().getIndexMap().getIndexRoot(index));
            } else {
                indexWriter.setRootPageId(-1);
            }
            indexWriter.setSegmentId(
                FtrsDataServer.getIndexSegmentId(index));
            indexWriter.setIndexId(
                JmiUtil.getObjectId(index));
            indexWriter.setTupleDesc(
                FtrsUtil.getCoverageTupleDescriptor(typeFactory,index));
            indexWriter.setKeyProj(
                FtrsUtil.getDistinctKeyProjection(catalog,index));
            indexWriter.setUpdateInPlace(updateInPlace);

            indexWriter.setDistinctness(
                index.isUnique()
                ? DistinctnessEnum.DUP_FAIL
                : DistinctnessEnum.DUP_ALLOW);
            if (index != clusteredIndex) {
                indexWriter.setInputProj(
                    FtrsUtil.getCoverageProjection(catalog,index));
            }

            boolean prepend = false;
            if (clusteredFirst) {
                if (index == clusteredIndex) {
                    prepend = true;
                }
            } else {
                if (catalog.isPrimary(index)) {
                    prepend = true;
                }
            }

            if (updateInPlace) {
                secondList.add(indexWriter);
            } else {
                if (prepend) {
                    firstList.add(0,indexWriter);
                } else {
                    firstList.add(indexWriter);
                }
            }

            // each BTreeWriter currently needs a private scratch page
            nPagesMin += 1;
        }

        // NOTE:  can't use addAll because MDR list impl doesn't support it;
        // TODO:  make a utility method instead
        Iterator iter = firstList.iterator();
        while (iter.hasNext()) {
            tableWriterDef.getIndexWriter().add(iter.next());
        }
        iter = secondList.iterator();
        while (iter.hasNext()) {
            tableWriterDef.getIndexWriter().add(iter.next());
        }

        tableWriterDef.setCachePageMin(nPagesMin);
        tableWriterDef.setCachePageMax(nPagesMin);

        // We only need a buffer if the target table is
        // also a source.
        boolean needBuffer = true;

        if (getOperation().equals(TableModificationRel.Operation.DELETE)) {
            needBuffer = false;
        } else {
            TableAccessMap tableAccessMap = new TableAccessMap(this);
            if (!tableAccessMap.isTableAccessedForRead(ftrsTable)) {
                needBuffer = false;
            }
        }

        if (needBuffer) {
            FemBufferingTupleStreamDef buffer =
                catalog.newFemBufferingTupleStreamDef();
            buffer.setInMemory(false);
            buffer.setMultipass(false);

            // only need one page for buffered stream access
            buffer.setCachePageMin(1);
            buffer.setCachePageMax(1);
            buffer.getInput().add(input);

            tableWriterDef.getInput().add(buffer);
        } else {
            tableWriterDef.getInput().add(input);
        }

        return tableWriterDef;
    }

    /**
     * .
     *
     * @return catalog for object definitions
     */
    FarragoCatalog getCatalog()
    {
        return getPreparingStmt().getCatalog();
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO:  say it's sorted instead.  This can be done generically for all
        // FennelRel's guaranteed to return at most one row
        return RelFieldCollation.emptyCollationArray;
    }
}


// End FtrsTableModificationRel.java
