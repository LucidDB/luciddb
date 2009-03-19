/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.resource.*;


/**
 * MedAbstractFennelTableModRel refines {@link TableModificationRelBase}. It is
 * also an abstract base class for implementations of the {@link FennelRel}
 * interface.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public abstract class MedAbstractFennelTableModRel
    extends TableModificationRelBase
    implements FennelRel
{
    //~ Constructors -----------------------------------------------------------

    public MedAbstractFennelTableModRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List<String> updateColumnList,
        boolean flattened)
    {
        super(
            cluster,
            traits,
            table,
            connection,
            child,
            operation,
            updateColumnList,
            flattened);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public RelOptConnection getConnection()
    {
        return connection;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(
            this,
            0,
            getChild());
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO:  say it's sorted instead.  This can be done generically for all
        // FennelRel's guaranteed to return at most one row
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    /**
     * Test if input needs to be buffered.
     *
     * @return true if input to this TableModificationRelBase needs to be
     * buffered.
     */
    public boolean inputNeedBuffer()
    {
        boolean needBuffer = false;

        // Except for Delete, if the target table is also the source, buffering
        // is required.
        if (getOperation() != TableModificationRel.Operation.DELETE) {
            if (isTableAccessedForRead()) {
                if (table instanceof MedAbstractColumnSet) {
                    CwmNamedColumnSet cwmColumnSet =
                        ((MedAbstractColumnSet) table).getCwmColumnSet();
                    if (cwmColumnSet instanceof CwmTable) {
                        CwmTable cwmTable = (CwmTable) cwmColumnSet;
                        needBuffer = isIndexRootSelfReferencing(cwmTable);
                    }
                }
            }
        }

        return needBuffer;
    }

    /*
     * Determines if an input needs to be buffered, taking into account
     * whether snapshot support is available.
     *
     * @param input the input
     */
    public boolean inputNeedBuffer(RelNode input)
    {
        // If real snapshot support is available, there's no need to buffer
        // the input, even if it's the same as the target table.  The
        // streams corresponding to the input just need to be set up so that
        // they only read committed data.
        if (FennelRelUtil.getPreparingStmt(input).getSession().getPersonality()
                         .supportsFeature(
                             EigenbaseResource.instance()
                             .PersonalitySupportsSnapshots))
        {
            return false;
        }

        // Deletes and merges always need buffering because they read the
        // table being operated on
        Operation op = getOperation();
        if ((op == TableModificationRel.Operation.DELETE)
            || (op == TableModificationRel.Operation.MERGE))
        {
            return true;
        }

        // All other cases depend on whether the source is the same as the
        // target.
        return inputNeedBuffer();
    }

    /**
     * @return whether the table to be modified is also accessed for reading
     */
    private boolean isTableAccessedForRead()
    {
        TableAccessMap tableAccessMap = new TableAccessMap(this);
        List<String> tableName = tableAccessMap.getQualifiedName(table);
        return tableAccessMap.isTableAccessedForRead(tableName);
    }

    /**
     * @return for a table modification that is self referencing (i.e. same
     * table is accessed for both input and output), determines whether any
     * index roots will be accessed for both input and output. In the case of
     * insertions, the deletion index is not taken into account, because
     * insertions produce no deleted entries.
     */
    private boolean isIndexRootSelfReferencing(CwmTable cwmTable)
    {
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);
        FarragoRepos repos = stmt.getRepos();
        FarragoSessionIndexMap indexMap =
            stmt.getSession().getSessionIndexMap();

        boolean selfReferencing = false;
        Collection<FemLocalIndex> indexes =
            FarragoCatalogUtil.getTableIndexes(repos, cwmTable);
        for (FemLocalIndex index : indexes) {
            if (getOperation().equals(TableModificationRel.Operation.INSERT)
                && FarragoCatalogUtil.isDeletionIndex(index))
            {
                continue;
            }
            long readRoot = indexMap.getIndexRoot(index, false);
            long writeRoot = indexMap.getIndexRoot(index, true);
            if (readRoot == writeRoot) {
                selfReferencing = true;
                break;
            }
        }
        return selfReferencing;
    }

    /**
     * Create a new buffering stream def based on input tuple format.
     *
     * @param repos repos storing object definitions.
     *
     * @return the newly constructed FemBufferingTupleStreamDef.
     */
    public FemBufferingTupleStreamDef newInputBuffer(FarragoRepos repos)
    {
        FemBufferingTupleStreamDef buffer =
            repos.newFemBufferingTupleStreamDef();

        buffer.setInMemory(false);
        buffer.setMultipass(false);

        buffer.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getFarragoTypeFactory(),
                getChild().getRowType()));
        return buffer;
    }
}

// End MedAbstractFennelTableModRel.java
