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
package net.sf.farrago.namespace.util;

import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;

import org.eigenbase.util.*;

/**
 * MedAbstractMetadataSink is an abstract base class for implementations
 * of the {@link FarragoMedMetadataSink} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractMetadataSink implements FarragoMedMetadataSink
{
    private final FarragoMedMetadataQuery query;
    private final FarragoTypeFactory typeFactory;
    
    /**
     * Creates a new sink.
     *
     * @param query the query being processed; its filters will be
     * implemented by this sink
     *
     * @param typeFactory factory for types written to this sink
     */
    protected MedAbstractMetadataSink(
        FarragoMedMetadataQuery query,
        FarragoTypeFactory typeFactory)
    {
        this.query = query;
        this.typeFactory = typeFactory;
    }

    /**
     * Tests whether an object should be included in the query result.
     *
     * @param objectName name of object
     *
     * @param typeName name of object type
     *
     * @param qualifier if true, test whether object is a valid qualifier;
     * if false, test whether object itself should be included
     *
     * @return true if the inclusion test passes
     */
    protected boolean shouldInclude(String objectName, String typeName,
        boolean qualifier)
    {
        if (!qualifier) {
            if (!query.getResultObjectTypes().contains(typeName)) {
                return false;
            }
        }
        FarragoMedMetadataFilter filter = (FarragoMedMetadataFilter)
            query.getFilterMap().get(typeName);
        if (filter == null) {
            return true;
        }
        if (filter.getRoster() != null) {
            boolean included = false;
            if (filter.getRoster().contains(objectName)) {
                included = true;
            }
            if (filter.isExclusion()) {
                included = !included;
            }
            return included;
        } else {
            // TODO jvs 6-Aug-2005:  share code with Java implementation
            // of LIKE expression
            throw Util.needToImplement("IMPORT FOREIGN SCHEMA with LIKE");
        }
    }
    
    // implement FarragoMedMetadataSink
    public FarragoTypeFactory getTypeFactory()
    {
        return typeFactory;
    }
}

// End MedAbstractMetadataSink.java
