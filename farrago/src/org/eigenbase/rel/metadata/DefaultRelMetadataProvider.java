/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.rel.metadata;

import java.util.*;

/**
 * DefaultRelMetadataProvider supplies a default implementation of the {@link
 * RelMetadataProvider} interface.  It provides generic formulas and derivation
 * rules for the standard logical algebra; coverage corresponds to the methods
 * declared in {@link RelMetadataQuery}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DefaultRelMetadataProvider extends ChainedRelMetadataProvider
{
    /**
     * Creates a new default provider.  Since this provider defines
     * "catch-all" handlers for generic RelNodes, extensions should be careful
     * to pass atEnd=false when using addProvider to set up additional custom
     * providers; otherwise, those providers will never get called.
     */
    public DefaultRelMetadataProvider()
    {
        boolean AT_END = true;
        
        addProvider(
            new RelMdPercentageOriginalRows(),
            Collections.singleton("getPercentageOriginalRows"),
            AT_END);
        
        addProvider(
            new RelMdColumnOrigins(),
            Collections.singleton("getColumnOrigins"),
            AT_END);
    }
}

// End DefaultRelMetadataProvider.java
