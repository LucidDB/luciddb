/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.rel.convert;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.util.Util;


/**
 * <code>NoneConverter</code> converts a plan from <code>inConvention</code>
 * to {@link org.eigenbase.relopt.CallingConvention#NONE_ORDINAL}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 15 February, 2002
 */
public class NoneConverterRel extends ConverterRel
{
    //~ Constructors ----------------------------------------------------------

    public NoneConverterRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.NONE;
    }

    public Object clone()
    {
        return new NoneConverterRel(this.cluster, child);
    }

    public static void init(RelOptPlanner planner)
    {
        // we can't convert from any conventions, therefore no rules to register
        Util.discard(planner);
    }
}


// End NoneConverterRel.java
