/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package org.eigenbase.rel;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * <code>MinusRel</code> returns the rows of its first input minus any matching
 * rows from its other inputs. If "all" is true, then multiset subtraction is
 * performed; otherwise, set subtraction is performed (implying no duplicates in
 * the results).
 *
 * @author jhyde
 * @version $Id$
 * @since 23 September, 2001
 */
public final class MinusRel
    extends SetOpRel
{
    //~ Constructors -----------------------------------------------------------

    public MinusRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        boolean all)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            inputs,
            all);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public double getRows()
    {
        // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
        double dRows = RelMetadataQuery.getRowCount(inputs[0]);
        for (int i = 1; i < inputs.length; i++) {
            dRows -= 0.5 * RelMetadataQuery.getRowCount(inputs[i]);
        }
        if (dRows < 0) {
            dRows = 0;
        }
        return dRows;
    }

    public MinusRel clone()
    {
        MinusRel clone =
            new MinusRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                all);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public MinusRel clone(RelNode [] inputs, boolean all)
    {
        MinusRel clone =
            new MinusRel(
                getCluster(),
                inputs,
                all);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End MinusRel.java
