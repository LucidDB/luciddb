/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

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
class FtrsTableModificationRel
    extends MedAbstractFennelTableModRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Refinement for TableModificationRel.table.
     */
    final FtrsTable ftrsTable;

    //~ Constructors -----------------------------------------------------------

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
        List<String> updateColumnList)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            ftrsTable,
            connection,
            child,
            operation,
            updateColumnList,
            true);
        this.ftrsTable = ftrsTable;
        assert ftrsTable.getPreparingStmt()
            == FennelRelUtil.getPreparingStmt(this);
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FtrsTableModificationRel clone()
    {
        FtrsTableModificationRel clone =
            new FtrsTableModificationRel(
                getCluster(),
                ftrsTable,
                getConnection(),
                getChild().clone(),
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

    private <E extends CwmColumn> List<E> getUpdateColumnList(Class<E> clazz)
    {
        int n = getUpdateColumnList().size();
        List<E> list = new ArrayList<E>();
        Collection<E> columns =
            Util.cast(
                ftrsTable.getCwmColumnSet().getFeature(),
                clazz);
        for (int i = 0; i < n; i++) {
            String columnName = getUpdateColumnList().get(i);
            E column =
                FarragoCatalogUtil.getModelElementByName(
                    columns,
                    columnName);
            list.add(column);
        }
        return list;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        if (!(ftrsTable.getCwmColumnSet() instanceof CwmTable)) {
            // e.g. view update
            throw Util.needToImplement(ftrsTable.getCwmColumnSet());
        }
        CwmTable table = (CwmTable) ftrsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        List<FemAbstractColumn> updateCwmColumnList = null;

        FemTableWriterDef tableWriterDef;
        switch (getOperation()) {
        case INSERT:
            tableWriterDef = repos.newFemTableInserterDef();
            break;
        case DELETE:
            tableWriterDef = repos.newFemTableDeleterDef();
            break;
        case UPDATE:
            FemTableUpdaterDef tableUpdaterDef = repos.newFemTableUpdaterDef();
            tableWriterDef = tableUpdaterDef;
            updateCwmColumnList = getUpdateColumnList(FemAbstractColumn.class);
            tableUpdaterDef.setUpdateProj(
                ftrsTable.getIndexGuide().createTupleProjectionFromColumnList(
                    updateCwmColumnList));
            break;
        default:
            throw Util.unexpected(getOperation());
        }

        // We need to be careful about the order in which we write to indexes.
        // This is required because secondary indexes rely on the clustering
        // key's uniqueness for discriminating entries, e.g. during rollback. So
        // when the clustering key is declared as unique, we should insert into
        // it first.  If a uniqueness violation is detected, we will abort the
        // insertion and the other indexes will be left untouched. However, when
        // the clustering key is not unique (except by virtue of its appended
        // primary key fields), then we should insert into the primary key index
        // first instead.  If that passes, the combination of the clustering key
        // with the primary key is guaranteed to be unique also.  For updates,
        // there is another consideration:  indexes which are updated in place
        // cannot be rolled back individually, so they must come last (after any
        // possible constraint violations).
        FemLocalIndex clusteredIndex =
            FarragoCatalogUtil.getClusteredIndex(repos, table);
        boolean clusteredFirst = clusteredIndex.isUnique();

        List<FemIndexWriterDef> firstList = new ArrayList<FemIndexWriterDef>();
        List<FemIndexWriterDef> secondList = new ArrayList<FemIndexWriterDef>();

        FtrsIndexGuide indexGuide = ftrsTable.getIndexGuide();

        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getTableIndexes(repos, table))
        {
            boolean updateInPlace = false;

            if (updateCwmColumnList != null) {
                if (!index.equals(clusteredIndex)) {
                    List<FemAbstractColumn> coverageList =
                        indexGuide.getUnclusteredCoverageColList(index);
                    if (!coverageList.removeAll(updateCwmColumnList)) {
                        // no intersection between update list and index
                        // coverage, so skip this index entirely
                        continue;
                    }
                }

                List<? extends Object> distinctKeyList =
                    indexGuide.getDistinctKeyColList(index);
                if (!distinctKeyList.removeAll(updateCwmColumnList)) {
                    // distinct key is not being changed, so it's safe to
                    // attempt update-in-place
                    updateInPlace = true;
                }
            }

            FemIndexWriterDef indexWriter =
                indexGuide.newIndexWriter(this, index);
            indexWriter.setUpdateInPlace(updateInPlace);
            if (!index.equals(clusteredIndex)) {
                indexWriter.setInputProj(
                    indexGuide.getCoverageProjection(index));
            }

            boolean prepend = false;
            if (clusteredFirst) {
                if (index.equals(clusteredIndex)) {
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
        for (FemIndexWriterDef indexWriter : firstList) {
            tableWriterDef.getIndexWriter().add(indexWriter);
        }
        for (FemIndexWriterDef indexWriter : secondList) {
            tableWriterDef.getIndexWriter().add(indexWriter);
        }

        // Set up buffering if required.
        // We only need a buffer if the target table is also a source.
        if (inputNeedBuffer()) {
            FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
            implementor.addDataFlowFromProducerToConsumer(
                input,
                buffer);
            input = buffer;
        }

        implementor.addDataFlowFromProducerToConsumer(
            input,
            tableWriterDef);

        return tableWriterDef;
    }
}

// End FtrsTableModificationRel.java
