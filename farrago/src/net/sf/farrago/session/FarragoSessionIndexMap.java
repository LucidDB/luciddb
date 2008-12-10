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
package net.sf.farrago.session;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;


/**
 * FarragoSessionIndexMap defines a means for mapping CWM index definitions to
 * corresponding physical storage.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionIndexMap
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets an index by its repository ID.
     *
     * @param id index ID in repository
     *
     * @return corresponding index
     */
    public FemLocalIndex getIndexById(long id);

    /**
     * Gets the root PageId of an index to be used for reading.
     *
     * @param index the index of interest
     *
     * @return root PageId as a long
     */
    public long getIndexRoot(FemLocalIndex index);

    /**
     * Gets the root PageId of an index to be used for reading or for writing.
     *
     * @param index the index of interest
     * @param write whether to access a root for reading or writing. A root for
     * reading reflects the index before modifications. A root for writing may
     * be the same root, or may reflect an updated index, depending on the
     * implementation of the session index map.
     *
     * @return root PageId as a long
     */
    public long getIndexRoot(FemLocalIndex index, boolean write);

    /**
     * Sets the root PageId of an index.
     *
     * @param index the index to be updated
     * @param pageId the root PageId
     */
    public void setIndexRoot(FemLocalIndex index, long pageId);

    /**
     * Called on every reference to a temporary table. Some implementations may
     * use this to create empty temporary indexes on first reference.
     *
     * @param wrapperCache cache for looking up data wrappers
     * @param table the temporary table
     */
    public void instantiateTemporaryTable(
        FarragoDataWrapperCache wrapperCache,
        CwmTable table);

    /**
     * For ALTER TABLE, retrieves the old table structure
     * corresponding to the table being modified.
     * Be warned that the old table structure is not a complete
     * copy; it consists of only the column definitions and their
     * datatypes (without default values).  Additional
     * logical constructs such as constraints are not present
     * on the returned object, nor are physical constructs
     * such as indexes.
     *
     * @return copy of old table, or null if not executing ALTER TABLE
     */
    public CwmTable getOldTableStructure();

    /**
     * Creates an index and records its root in this map.
     *
     * @param wrapperCache cache for looking up data wrappers
     * @param index the index to create
     */
    public void createIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index);

    /**
     * Creates an index and optionally records its root in this map.
     *
     * @param wrapperCache cache for looking up data wrappers
     * @param index the index to create
     * @param updateMap whether to record the new root in the map
     *
     * @return the root of the newly created index storage
     */
    public long createIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean updateMap);

    /**
     * Drops an index and removes its root from this map.
     *
     * @param wrapperCache cache for looking up data wrappers
     * @param index the index to drop
     * @param truncate if true, only truncate storage; if false, drop storage
     * entirely
     */
    public void dropIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean truncate);

    /**
     * Verifies an index and records returns page count for the index.
     *
     * @param wrapperCache cache for looking up data wrappers
     * @param index the index to verify
     *
     * @return page count for the index
     */
    public FarragoMedLocalIndexStats computeIndexStats(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean estimate);

    /**
     * Commit hook.
     */
    public void onCommit();

    /**
     * Versions an index root page.
     *
     * @param wrapperCache Wrapper cache
     * @param index Index definition
     * @param newRoot new index root ids
     */
    public void versionIndexRoot(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        Long newRoot);
}

// End FarragoSessionIndexMap.java
