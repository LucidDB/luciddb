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
package net.sf.farrago.namespace.impl;

import java.util.List;

import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.fem.fennel.FemBufferingTupleStreamDef;
import net.sf.farrago.namespace.FarragoMedDataServer;
import net.sf.farrago.query.*;
import net.sf.farrago.type.FarragoTypeFactory;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * MedAbstractFennelTableModRel refines {@link TableModificationRelbase}.
 * It is also an abstract base class for implementations of the {@link FennelRel} interface.
 * 
 * @author Rushan Chen
 * @version $Id$
 */
public abstract class MedAbstractFennelTableModRel 
    extends TableModificationRelBase 
    implements FennelRel
{
    //~ Constructors ----------------------------------------------------------

    public MedAbstractFennelTableModRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List updateColumnList,
        boolean flattened)
    {
        super(
            cluster, traits, table,
            connection, child, operation, updateColumnList, flattened);
    }
    //~ Methods ---------------------------------------------------------------

    // implement FennelRel
    public RelOptConnection getConnection()
    {
        return connection;
    }
    
    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, getChild());
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
     * @return true if input to this TableModificationRelBase needs to be buffered.
     */
    public boolean inputNeedBuffer()
    {
        boolean needBuffer = false;

        //
        // Except for Delete, if the target table is also the source, buffering
        // is required.
        //
        if (!getOperation().equals(TableModificationRel.Operation.DELETE)) {
            TableAccessMap tableAccessMap = new TableAccessMap(this);
            List<String> tableName = tableAccessMap.getQualifiedName(table);
            if (tableAccessMap.isTableAccessedForRead(tableName)) {
                needBuffer = true;
            }
        }
        
       return needBuffer; 
    }
    
    /**
     * Create a new buffering stream def based on input tuple format.
     * 
     * @param repos repos storing object definitions.
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

//End MedAbstractFennelTableModRel.java
