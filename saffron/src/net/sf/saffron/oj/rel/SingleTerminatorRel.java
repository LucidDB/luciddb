/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SingleRel;


/**
 * A <code>SingleTerminatorRel</code> is an abstract base for {@link
 * Terminator}s with one input.
 */
public abstract class SingleTerminatorRel extends SingleRel
    implements TerminatorRel
{
    //~ Constructors ----------------------------------------------------------

    SingleTerminatorRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        return planner.makeTinyCost();
    }

    // implement SaffronRel
    protected SaffronType deriveRowType()
    {
        throw new UnsupportedOperationException();
    }
}


// End SingleTerminatorRel.java
