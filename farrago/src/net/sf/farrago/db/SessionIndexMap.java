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

package net.sf.farrago.db;

import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.query.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;

import java.util.*;

/**
 * SessionIndexMap implements FarragoIndexMap, resolving indexes for both
 * permanent and temporary tables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SessionIndexMap implements FarragoIndexMap, FarragoAllocation
{
    private FarragoDatabase database;
    
    /**
     * Map from index to root PageId for temporary tables.
     */
    private Map tempIndexRootMap;
    
    /**
     * Map from index ID to index for temporary tables.
     */
    private Map tempIndexIdMap;
    
    /**
     * Private type factory.  REVIEW:  use a sharable one instead?
     */
    private FarragoTypeFactory typeFactory;

    /**
     * Catalog for this session.
     */
    private FarragoCatalog catalog;

    /**
     * Create a new SessionIndexMap.
     *
     * @param owner FarragoAllocationOwner which will own this map; on
     * closeAllocation, any temporary indexes will be deleted automatically
     *
     * @param database FarragoDatabase context
     *
     * @param catalog the catalog for this session
     */
    public SessionIndexMap(
        FarragoAllocationOwner owner,
        FarragoDatabase database,
        FarragoCatalog catalog)
    {
        this.database = database;
        this.catalog = catalog;
        tempIndexRootMap = new HashMap();
        tempIndexIdMap = new HashMap();
        typeFactory = new FarragoTypeFactoryImpl(catalog);
        owner.addAllocation(this);
    }
    
    // implement FarragoIndexMap
    public long getIndexRoot(
        CwmSqlindex index)
    {
        if (isTemporary(index)) {
            Long root = (Long) tempIndexRootMap.get(index);
            assert(root != null);
            return root.longValue();
        } else {
            return Long.parseLong(
                catalog.getTagValue(index,"indexRoot"));
        }
    }
    
    private void setIndexRoot(
        CwmSqlindex index,long root)
    {
        if (isTemporary(index)) {
            Object old = tempIndexRootMap.put(index,new Long(root));
            assert(old == null);
        } else {
            catalog.setTagValue(
                index,"indexRoot",Long.toString(root));
        }
    }

    // implement FarragoIndexMap
    public void instantiateTemporaryTable(
        CwmTable table)
    {
        assert(table.isTemporary());
        
        CwmSqlindex clusteredIndex = catalog.getClusteredIndex(table);

        if (tempIndexRootMap.containsKey(clusteredIndex)) {
            // already instantiated this table
            return;
        }

        Iterator iter = catalog.getIndexes(table).iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();
            assert(!tempIndexRootMap.containsKey(index));
            createIndexStorage(index);
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        // materialize deletion list to avoid ConcurrentModificationException
        List list = new ArrayList(tempIndexRootMap.keySet());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();
            dropIndexStorage(index,false);
        }
        // TODO:  make Fennel drop temporary indexes on recovery also
    }

    /**
     * Commit hook:  truncate any indexes for tables defined with
     * ON COMMIT DELETE ROWS.
     */
    public void onCommit()
    {
        Iterator iter = tempIndexRootMap.keySet().iterator();
        while (iter.hasNext()) {
            CwmSqlindexImpl index = (CwmSqlindexImpl) iter.next();
            if (index.getTable().getTemporaryScope().endsWith("PRESERVE")) {
                continue;
            }
            dropIndexStorage(index,true);
        }
    }

    // REVIEW:  rollback issues

    public boolean isTemporary(CwmSqlindex index)
    {
        return ((CwmTable) index.getSpannedClass()).isTemporary();
    }

    private void initIndexCmd(
        FemIndexCmd cmd,CwmSqlindex index)
    {
        cmd.setDbHandle(database.getFennelDbHandle().getFemDbHandle(catalog));
        cmd.setTupleDesc(
            FennelRelUtil.getCoverageTupleDescriptor(
                typeFactory,
                index));
        cmd.setKeyProj(
            FennelRelUtil.getDistinctKeyProjection(catalog,index));
        cmd.setSegmentId(getIndexSegmentId(index));
        cmd.setIndexId(JmiUtil.getObjectId(index));
    }

    // implement FarragoIndexMap
    public long getIndexSegmentId(
        CwmSqlindex index)
    {
        // TODO:  share symbolic enum with Fennel rather than hard-coding
        // values here
        if (isTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }
    
    // implement FarragoIndexMap
    public void createIndexStorage(
        CwmSqlindex index)
    {
        FemCmdCreateIndex cmd = catalog.newFemCmdCreateIndex();
        if (!catalog.isFennelEnabled()) {
            return;
        }
        
        initIndexCmd(cmd,index);
        long indexRoot = database.getFennelDbHandle().executeCmd(cmd);
        setIndexRoot(index,indexRoot);
        tempIndexIdMap.put(
            new Long(JmiUtil.getObjectId(index)),
            index);
    }

    // implement FarragoIndexMap
    public void dropIndexStorage(
        CwmSqlindex index,boolean truncate)
    {
        if (isTemporary(index)) {
            if (!tempIndexRootMap.containsKey(index)) {
                // index was never created, so nothing to do
                return;
            }
        }
        
        FemCmdDropIndex cmd;
        if (truncate) {
            cmd = catalog.newFemCmdTruncateIndex();
        } else {
            cmd = catalog.newFemCmdDropIndex();
        }
        if (!catalog.isFennelEnabled()) {
            return;
        }
        initIndexCmd(cmd,index);
        cmd.setRootPageId(getIndexRoot(index));
        database.getFennelDbHandle().executeCmd(cmd);
        if (!truncate) {
            tempIndexIdMap.remove(new Long(JmiUtil.getObjectId(index)));
            tempIndexRootMap.remove(index);
        }
    }
    
    // implement FarragoIndexMap
    public CwmSqlindex getIndexById(long id)
    {
        return (CwmSqlindex) tempIndexIdMap.get(new Long(id));
    }
}

// End SessionIndexMap.java
