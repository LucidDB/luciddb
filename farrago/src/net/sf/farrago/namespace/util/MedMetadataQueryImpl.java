/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import java.util.*;

import net.sf.farrago.namespace.*;


/**
 * MedMetadataQueryImpl is a default implementation for {@link
 * FarragoMedMetadataQuery}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMetadataQueryImpl
    implements FarragoMedMetadataQuery
{
    //~ Instance fields --------------------------------------------------------

    private final Map<String, FarragoMedMetadataFilter> filterMap;

    private final Set<String> resultObjectTypes;

    //~ Constructors -----------------------------------------------------------

    public MedMetadataQueryImpl()
    {
        filterMap = new HashMap<String, FarragoMedMetadataFilter>();
        resultObjectTypes = new HashSet<String>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedMetadataQuery
    public Map<String, FarragoMedMetadataFilter> getFilterMap()
    {
        return filterMap;
    }

    // implement FarragoMedMetadataQuery
    public Set<String> getResultObjectTypes()
    {
        return resultObjectTypes;
    }
}

// End MedMetadataQueryImpl.java
