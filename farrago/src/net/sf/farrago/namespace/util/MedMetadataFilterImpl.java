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
 * MedMetadataFilterImpl is a default implementation {@link
 * FarragoMedMetadataFilter}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMetadataFilterImpl
    implements FarragoMedMetadataFilter
{
    //~ Instance fields --------------------------------------------------------

    private final boolean exclude;

    private final Set roster;

    private final String pattern;

    //~ Constructors -----------------------------------------------------------

    public MedMetadataFilterImpl(
        boolean exclude,
        Set roster,
        String pattern)
    {
        this.exclude = exclude;
        this.roster = roster;
        this.pattern = pattern;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedMetadataFilter
    public boolean isExclusion()
    {
        return exclude;
    }

    // implement FarragoMedMetadataFilter
    public Set getRoster()
    {
        return roster;
    }

    // implement FarragoMedMetadataFilter
    public String getPattern()
    {
        return pattern;
    }
}

// End MedMetadataFilterImpl.java
