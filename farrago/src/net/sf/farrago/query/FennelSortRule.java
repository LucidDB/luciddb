/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.query;

import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

import openjava.ptree.*;

import java.util.*;


/**
 * FennelSortRule is a rule for implementing SortRel via a Fennel
 * sort.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelSortRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelSortRule object.
     */
    public FennelSortRule()
    {
        super(
            new RuleOperand(
                SortRel.class,
                new RuleOperand [] { new RuleOperand(SaffronRel.class,null) }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        SortRel sortRel = (SortRel) call.rels[0];
        SaffronRel relInput = call.rels[1];
        SaffronRel fennelInput =
            convert(relInput,FennelPullRel.FENNEL_PULL_CONVENTION);
        if (fennelInput == null) {
            return;
        }

        Integer [] keyProjection = new Integer[sortRel.getCollations().length];
        for (int i = 0; i < keyProjection.length; ++i) {
            keyProjection[i] = new Integer(
                sortRel.getCollations()[i].iField);
        }

        boolean discardDuplicates = false;
        FennelSortRel fennelSortRel =
            new FennelSortRel(
                sortRel.getCluster(),
                fennelInput,
                keyProjection,
                discardDuplicates);
        call.transformTo(fennelSortRel);
    }
}


// End FennelSortRule.java
