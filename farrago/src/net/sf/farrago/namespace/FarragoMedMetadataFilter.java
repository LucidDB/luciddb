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


/**
 * FarragoMedMetadataFilter represents a filter on a {@link
 * FarragoMedMetadataQuery}. A filter may be expressed as either an explicit
 * name roster or a name pattern.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataFilter
{

    //~ Methods ----------------------------------------------------------------

    /**
     * @return true if objects matching filter are to be excluded from query
     * results; false if only objects matching filter are to be included in the
     * results
     */
    public boolean isExclusion();

    /**
     * @return Set<String> representing filter membership, or null for a pattern
     * filter
     */
    public Set getRoster();

    /**
     * @return LIKE pattern, or null for a roster filter
     */
    public String getPattern();
}

// End FarragoMedMetadataFilter.java
