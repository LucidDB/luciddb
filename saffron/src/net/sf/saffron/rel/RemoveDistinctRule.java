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

import net.sf.saffron.opt.*;
import net.sf.saffron.util.Util;


/**
 * Rule to remove a {@link DistinctRel} if the underlying relational expression
 * is already distinct, otherwise replace it with an AggregateRel.
 */
public class RemoveDistinctRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    public RemoveDistinctRule()
    {
        super(
            new RuleOperand(
                DistinctRel.class,
                new RuleOperand [] { new RuleOperand(SaffronRel.class,null) }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
    {
        DistinctRel distinct = (DistinctRel) call.rels[0];
        Util.discard(distinct);
        SaffronRel child = call.rels[1];
        if (child.isDistinct()) {
            call.transformTo(child);
        } else {
            call.transformTo(
                new AggregateRel(
                    child.getCluster(),
                    child,
                    child.getRowType().getFieldCount(),
                    new AggregateRel.Call[0]));
        }
    }
}


// End RemoveDistinctRule.java
