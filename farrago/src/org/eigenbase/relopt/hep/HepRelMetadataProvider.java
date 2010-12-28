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
package org.eigenbase.relopt.hep;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;


/**
 * HepRelMetadataProvider implements the {@link RelMetadataProvider} interface
 * by combining metadata from the rels inside of a {@link HepRelVertex}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class HepRelMetadataProvider
    implements RelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        if (!(rel instanceof HepRelVertex)) {
            return null;
        }

        HepRelVertex vertex = (HepRelVertex) rel;

        // Use current implementation.
        Object result =
            rel.getCluster().getMetadataProvider().getRelMetadata(
                vertex.getCurrentRel(),
                metadataQueryName,
                args);
        return result;
    }
}

// End HepRelMetadataProvider.java
