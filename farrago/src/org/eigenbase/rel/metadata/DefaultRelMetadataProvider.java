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

import org.eigenbase.util.*;
import org.eigenbase.rel.*;

import java.util.*;

/**
 * DefaultRelMetadataProvider supplies a default implementation of the {@link
 * RelMetadataProvider} interface.  It provides generic formulas and derivation
 * rules for the standard logical algebra; coverage corresponds to the methods
 * declared in {@link RelMetadataQuery}.  It also supplies caching.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DefaultRelMetadataProvider extends ChainedRelMetadataProvider
{
    private final Map<List, Object> cache;
    
    /**
     * Creates a new default provider.  This provider defines
     * "catch-all" handlers for generic RelNodes, so it should
     * always be given lowest priority when chaining.
     */
    public DefaultRelMetadataProvider()
    {
        addProvider(
            new RelMdPercentageOriginalRows());
        
        addProvider(
            new RelMdColumnOrigins());

        cache = new HashMap<List, Object>();
    }


    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        // REVIEW jvs 29-Mar-2006:  Some queries may not be
        // cacheable, or may need to have their cached values
        // flushed.  That requires some meta-metadata, plus
        // interaction with the planner.
        
        // Check cache first.
        List hashKey;
        if (args != null) {
            hashKey = new ArrayList(args.length + 2);
            hashKey.add(rel);
            hashKey.add(metadataQueryName);
            hashKey.addAll(Arrays.asList(args));
        } else {
            hashKey = Arrays.asList(rel, metadataQueryName);
        }
        Object result = cache.get(hashKey);
        if (result != null) {
            return result;
        }

        // Cache miss.
        result = super.getRelMetadata(rel, metadataQueryName, args);
        if (result != null) {
            cache.put(hashKey, result);
        }
        return result;
    }
}

// End DefaultRelMetadataProvider.java
