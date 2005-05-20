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
package net.sf.farrago.session;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
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
    //~ Methods ---------------------------------------------------------------

    /**
     * Gets an index by its repository ID.
     *
     * @param id index ID in repository
     *
     * @return corresponding index
     */
    public FemLocalIndex getIndexById(long id);

    /**
     * Gets the root PageId of an index.
     *
     * @param index the index of interest
     *
     * @return root PageId as a long
     */
    public long getIndexRoot(FemLocalIndex index);

    /**
     * Called on every reference to a temporary table.  Some implementations
     * may use this to create empty temporary indexes on first reference.
     *
     * @param wrapperCache cache for looking up data wrappers
     *
     * @param table the temporary table
     */
    public void instantiateTemporaryTable(
        FarragoDataWrapperCache wrapperCache,
        CwmTable table);

    /**
     * Creates an index and records its root in this map.
     *
     * @param wrapperCache cache for looking up data wrappers
     *
     * @param index the index to create
     */
    public void createIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index);

    /**
     * Drops an index and removes its root from this map.
     *
     * @param wrapperCache cache for looking up data wrappers
     *
     * @param index the index to drop
     *
     * @param truncate if true, only truncate storage; if false, drop storage
     * entirely
     */
    public void dropIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean truncate);
}


// End FarragoSessionIndexMap.java
