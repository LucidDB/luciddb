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

package net.sf.saffron.rel;

import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;


/**
 * <code>UnionToDistinctRule</code> translates a distinct {@link UnionRel}
 * (<code>all</code> = <code>false</code>) into a {@link DistinctRel} on top
 * of a non-distinct {@link UnionRel} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    public UnionToDistinctRule()
    {
        super(new RuleOperand(UnionRel.class,null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
    {
        UnionRel union = (UnionRel) call.rels[0];
        if (union.getClass() != UnionRel.class) {
            return; // require precise class, otherwise we loop
        }
        if (union.all) {
            return; // nothing to do
        }
        UnionRel unionAll = new UnionRel(union.cluster,union.inputs,true);
        call.transformTo(new DistinctRel(union.cluster,unionAll));
    }
}


// End UnionToDistinctRule.java
