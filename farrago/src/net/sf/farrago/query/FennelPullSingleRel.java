/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.query;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelPullSingleRel is a {@link FennelSingleRel} which is also a
 * {@link FennelPullRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelPullSingleRel extends FennelSingleRel
    implements FennelPullRel
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelPullSingleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel
     */
    protected FennelPullSingleRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }
}


// End FennelPullSingleRel.java
