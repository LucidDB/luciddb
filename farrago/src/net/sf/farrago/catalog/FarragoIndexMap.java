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

package net.sf.farrago.catalog;

import net.sf.farrago.cwm.relational.*;

/**
 * FarragoIndexMap defines a means for mapping CWM index definitions to
 * corresponding physical storage.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoIndexMap
{
    /**
     * Get an index by its ID.
     *
     * @param id index ID
     *
     * @return corresponding ID
     */
    public CwmSqlindex getIndexById(long id);
    
    /**
     * Get the root PageId of an index.
     *
     * @param index the index of interest
     *
     * @return root PageId as a long
     */
    public long getIndexRoot(
        CwmSqlindex index);

    /**
     * Called on every reference to a temporary table.  Some implementations
     * may use this to create empty temporary indexes on first reference.
     *
     * @param table the temporary table
     */
    public void instantiateTemporaryTable(
        CwmTable table);

    /**
     * Create an index and record its root in this map.
     *
     * @param index the index to create
     */
    public void createIndexStorage(
        CwmSqlindex index);
    
    /**
     * Drop an index and remove its root from this map.
     *
     * @param index the index to drop
     *
     * @param truncate if true, only truncate storage; if false, drop storage
     * entirely
     */
    public void dropIndexStorage(
        CwmSqlindex index,
        boolean truncate);
}

// End FarragoIndexMap.java
