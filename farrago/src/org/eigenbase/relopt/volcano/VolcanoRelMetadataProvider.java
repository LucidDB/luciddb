/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package org.eigenbase.relopt.volcano;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;


/**
 * VolcanoRelMetadataProvider implements the {@link RelMetadataProvider}
 * interface by combining metadata from the rels making up an equivalence class.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class VolcanoRelMetadataProvider
    implements RelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        if (!(rel instanceof RelSubset)) {
            // let someone else further down the chain sort it out
            return null;
        }

        RelSubset subset = (RelSubset) rel;

        // REVIEW jvs 29-Mar-2006: I'm not sure what the correct precedence
        // should be here.  Letting the current best plan take the first shot is
        // probably the right thing to do for physical estimates such as row
        // count.  Dunno about others, and whether we need a way to
        // discriminate.

        // First, try current best implementation.  If it knows how to answer
        // this query, treat it as the most reliable.
        if (subset.best != null) {
            Object result =
                rel.getCluster().getMetadataProvider().getRelMetadata(
                    subset.best,
                    metadataQueryName,
                    args);
            if (result != null) {
                return result;
            }
        }

        // Otherwise, try rels in same logical equivalence class to see if any
        // of them have a good answer.  We use the full logical equivalence
        // class rather than just the subset because many metadata providers
        // only know about logical metadata.

        // Equivalence classes can get tangled up in interesting ways, so avoid
        // an infinite loop.  REVIEW: There's a chance this will cause us to
        // fail on metadata queries which invoke other queries, e.g.
        // PercentageOriginalRows -> Selectivity.  If we implement caching at
        // this level, we could probably kill two birds with one stone (use
        // presence of pending cache entry to detect reentrancy at the correct
        // granularity).
        if (subset.set.inMetadataQuery) {
            return null;
        }

        subset.set.inMetadataQuery = true;
        try {
            for (RelNode relCandidate : subset.set.rels) {
                Object result =
                    rel.getCluster().getMetadataProvider().getRelMetadata(
                        relCandidate,
                        metadataQueryName,
                        args);
                if (result != null) {
                    return result;
                }
            }
        } finally {
            subset.set.inMetadataQuery = false;
        }

        // Give up.
        return null;
    }
}

// End VolcanoRelMetadataProvider.java
