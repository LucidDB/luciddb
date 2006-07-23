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

import org.eigenbase.rel.*;


/**
 * RelMetadataProvider defines an interface for obtaining metadata about
 * relational expressions. This interface is weakly-typed and is not intended to
 * be called directly in most contexts; instead, use a strongly-typed facade
 * such as {@link RelMetadataQuery}.
 *
 * <p>For background and motivation, see <a
 * href="http://wiki.eigenbase.org/RelationalExpressionMetadata">wiki</a>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface RelMetadataProvider
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves metadata about a relational expression.
     *
     * @param rel relational expression of interest
     * @param metadataQueryName name of metadata query to invoke
     * @param args arguments to metadata query (expected number and type depend
     * on query name; must have well-defined hashCode/equals for use by
     * caching); null can be used instead of empty array
     *
     * @return metadata result (actual type depends on query name), or null if
     * the provider cannot answer the given query/rel combination; it is better
     * to return null than to return a possibly incorrect answer
     */
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args);
}

// End RelMetadataProvider.java
