/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

/**
 * DefaultRelMetadataProvider supplies a default implementation of the {@link
 * RelMetadataProvider} interface. It provides generic formulas and derivation
 * rules for the standard logical algebra; coverage corresponds to the methods
 * declared in {@link RelMetadataQuery}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DefaultRelMetadataProvider
    extends ChainedRelMetadataProvider
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new default provider. This provider defines "catch-all"
     * handlers for generic RelNodes, so it should always be given lowest
     * priority when chaining.
     */
    public DefaultRelMetadataProvider()
    {
        addProvider(new RelMdPercentageOriginalRows());

        addProvider(new RelMdColumnOrigins());

        addProvider(new RelMdRowCount());

        addProvider(new RelMdUniqueKeys());

        addProvider(new RelMdColumnUniqueness());

        addProvider(new RelMdPopulationSize());

        addProvider(new RelMdDistinctRowCount());

        addProvider(new RelMdSelectivity());
    }
}

// End DefaultRelMetadataProvider.java
