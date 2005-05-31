/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * FtrsTableModificationRel is the relational expression corresponding to
 * modification of a FTRS table.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableModificationRel extends TableModificationRelBase
    implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    /** Refinement for TableModificationRel.table. */
    final FtrsTable ftrsTable;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableModificationRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param ftrsTable target table
     * @param connection connection expression
     * @param child child producing rows to be inserted
     * @param operation modification operation to perform
     * @param updateColumnList list of column names to be updated
     */
    public FtrsTableModificationRel(
        RelOptCluster cluster,
        FtrsTable ftrsTable,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List updateColumnList)
    {
        super(cluster, new RelTraitSet(FennelPullRel.FENNEL_PULL_CONVENTION),
            ftrsTable, connection, child, operation, updateColumnList, true);
        this.ftrsTable = ftrsTable;
        assert ftrsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FennelRel
    public RelOptConnection getConnection()
    {
        return connection;
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) cluster.typeFactory;
    }

    // implement Cloneable
    public Object clone()
    {
        FtrsTableModificationRel clone = new FtrsTableModificationRel(
            cluster,
            ftrsTable,
            getConnection(),
            RelOptUtil.clone(child),
            getOperation(),
            getUpdateColumnList());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing
        return planner.makeTinyCost();
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, child);
    }

    private List getUpdateCwmColumnList()
    {
        int n = getUpdateColumnList().size();
        List list = new ArrayList();
        Collection columns = ftrsTable.getCwmColumnSet().getFeature();
        for (int i = 0; i < n; i++) {
            String columnName = (String) getUpdateColumnList().get(i);
            CwmColumn column =
                (CwmColumn) FarragoCatalogUtil.getModelElementByName(
                    columns, columnName);
            list.add(column);
        }
        return list;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) child);
        FarragoTypeFactory typeFactory = getFarragoTypeFactory();
        if (!(ftrsTable.getCwmColumnSet() instanceof CwmTable)) {
            // e.g. view update
            throw Util.needToImplement(ftrsTable.getCwmColumnSet());
        }
        CwmTable table = (CwmTable) ftrsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        List updateCwmColumnList = null;

        FemTableWriterDef tableWriterDef;
        switch (getOperation().getOrdinal()) {
        case TableModificationRel.Operation.INSERT_ORDINAL:
            tableWriterDef = repos.newFemTableInserterDef();
            break;
        case TableModificationRel.Operation.DELETE_ORDINAL:
            tableWriterDef = repos.newFemTableDeleterDef();
            break;
        case TableModificationRel.Operation.UPDATE_ORDINAL:
            FemTableUpdaterDef tableUpdaterDef = repos.newFemTableUpdaterDef();
            tableWriterDef = tableUpdaterDef;
            updateCwmColumnList = getUpdateCwmColumnList();
            tableUpdaterDef.setUpdateProj(
                ftrsTable.getIndexGuide().createTupleProjectionFromColumnList(
                    updateCwmColumnList));
            break;
        default:
            throw getOperation().unexpected();
        }

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
        FemLocalIndex clusteredIndex =
            FarragoCatalogUtil.getClusteredIndex(repos, table);
        boolean clusteredFirst = clusteredIndex.isUnique();

        List firstList = new ArrayList();
        List secondList = new ArrayList();

        FtrsIndexGuide indexGuide = ftrsTable.getIndexGuide();

        Iterator indexIter = FarragoCatalogUtil.getTableIndexes(
            repos, table).iterator();
        while (indexIter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) indexIter.next();

            boolean updateInPlace = false;

            if (updateCwmColumnList != null) {
                if (index != clusteredIndex) {
                    List coverageList =
                        indexGuide.getUnclusteredCoverageColList(index);
                    if (!coverageList.removeAll(updateCwmColumnList)) {
                        // no intersection between update list and index
                        // coverage, so skip this index entirely
                        continue;
                    }
                }

                List distinctKeyList = indexGuide.getDistinctKeyColList(index);
                if (!distinctKeyList.removeAll(updateCwmColumnList)) {
                    // distinct key is not being changed, so it's safe to
                    // attempt update-in-place
                    updateInPlace = true;
                }
            }

            FemIndexWriterDef indexWriter =
                repos.getFennelPackage().getFemIndexWriterDef()
                    .createFemIndexWriterDef();
            if (!FarragoCatalogUtil.isIndexTemporary(index)) {
                final FarragoPreparingStmt stmt =
                    FennelRelUtil.getPreparingStmt(this);
                indexWriter.setRootPageId(
                    stmt.getIndexMap().getIndexRoot(index));
            } else {
                indexWriter.setRootPageId(-1);
            }
            indexWriter.setSegmentId(FtrsDataServer.getIndexSegmentId(index));
            indexWriter.setIndexId(JmiUtil.getObjectId(index));
            indexWriter.setTupleDesc(
                indexGuide.getCoverageTupleDescriptor(index));
            indexWriter.setKeyProj(
                indexGuide.getDistinctKeyProjection(index));
            indexWriter.setUpdateInPlace(updateInPlace);

            indexWriter.setDistinctness(index.isUnique()
                ? DistinctnessEnum.DUP_FAIL : DistinctnessEnum.DUP_ALLOW);
            if (index != clusteredIndex) {
                indexWriter.setInputProj(
                    indexGuide.getCoverageProjection(index));
            }

            boolean prepend = false;
            if (clusteredFirst) {
                if (index == clusteredIndex) {
                    prepend = true;
                }
            } else {
                if (FarragoCatalogUtil.isIndexPrimaryKey(index)) {
                    prepend = true;
                }
            }

            if (updateInPlace) {
                secondList.add(indexWriter);
            } else {
                if (prepend) {
                    firstList.add(0, indexWriter);
                } else {
                    firstList.add(indexWriter);
                }
            }
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
                repos.newFemBufferingTupleStreamDef();
            buffer.setInMemory(false);
            buffer.setMultipass(false);
            buffer.setOutputDesc(
                FennelRelUtil.createTupleDescriptorFromRowType(
                    repos,
                    getFarragoTypeFactory(),
                    child.getRowType()));

            buffer.getInput().add(input);

            tableWriterDef.getInput().add(buffer);
        } else {
            tableWriterDef.getInput().add(input);
        }

        return tableWriterDef;
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
