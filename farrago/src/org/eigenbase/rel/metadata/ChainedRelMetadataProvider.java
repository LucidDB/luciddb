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

import org.eigenbase.rel.*;


/**
 * ChainedRelMetadataProvider implements the {@link RelMetadataProvider}
 * interface via the {@link
 * org.eigenbase.util.Glossary#ChainOfResponsibilityPattern}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ChainedRelMetadataProvider
    implements RelMetadataProvider
{
    //~ Instance fields --------------------------------------------------------

    private List<RelMetadataProvider> providers;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty chain.
     */
    public ChainedRelMetadataProvider()
    {
        providers = new ArrayList<RelMetadataProvider>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a provider, giving it higher priority than all those already in
     * chain. Chain order matters, since the first provider which answers a
     * query is used.
     *
     * @param provider provider to add
     */
    public void addProvider(
        RelMetadataProvider provider)
    {
        providers.add(0, provider);
    }

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        for (RelMetadataProvider provider : providers) {
            Object result =
                provider.getRelMetadata(
                    rel,
                    metadataQueryName,
                    args);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}

// End ChainedRelMetadataProvider.java
