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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * <code>ValuesRel</code> represents a sequence of zero or more literal row
 * values.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ValuesRel
    extends ValuesRelBase
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new ValuesRel. Note that tuples passed in become owned by this
     * rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster .
     * @param rowType row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be of
     * same length, conforming to rowType
     */
    public ValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples)
    {
        super(
            cluster,
            rowType,
            tuples,
            new RelTraitSet(CallingConvention.NONE));
    }
}

// End ValuesRel.java
