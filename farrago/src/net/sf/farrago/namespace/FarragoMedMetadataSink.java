/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.namespace;

import java.util.*;

import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;


/**
 * FarragoMedMetadataSink provides a target for instances of {@link
 * FarragoMedNameDirectory} to write metadata results in response to a {@link
 * FarragoMedMetadataQuery}. Results must be written in dependency order (e.g. a
 * table before its columns), and columns must be written in ordinal order.
 *
 * <p>Results may be filtered as they are written, in which case the sink
 * reports the filter result back to the caller.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataSink
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Writes a generic descriptor for an object. Some objects (such as columns)
     * have more specific write methods.
     *
     * @param name unqualified object name
     * @param remarks object description, or null for none
     * @param properties storage options
     *
     * @return true if object was accepted; false if object was filtered out
     */
    public boolean writeObjectDescriptor(
        String name,
        String typeName,
        String remarks,
        Properties properties);

    /**
     * Writes a descriptor for a column.
     *
     * @param tableName unqualified table name
     * @param columnName unqualified column name
     * @param ordinal 0-based ordinal of column within table
     * @param type column datatype
     * @param remarks column description, or null for none
     * @param defaultValue column default value, or null for none
     * @param properties storage options
     *
     * @return true if object was accepted; false if object was filtered out
     */
    public boolean writeColumnDescriptor(
        String tableName,
        String columnName,
        int ordinal,
        RelDataType type,
        String remarks,
        String defaultValue,
        Properties properties);

    /**
     * @return a type factory for use in creating type instances for calls such
     * as {@link #writeColumnDescriptor}
     */
    public FarragoTypeFactory getTypeFactory();
}

// End FarragoMedMetadataSink.java
