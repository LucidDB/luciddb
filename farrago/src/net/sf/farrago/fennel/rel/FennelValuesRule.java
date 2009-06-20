/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package net.sf.farrago.fennel.rel;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;


/**
 * FennelValuesRule provides an implementation for {@link ValuesRel} in terms of
 * {@link FennelValuesRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelValuesRule
    extends ConverterRule
{
    public static final FennelValuesRule instance =
        new FennelValuesRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelValuesRule.
     */
    private FennelValuesRule()
    {
        super(
            ValuesRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelValuesRule");
    }

    //~ Methods ----------------------------------------------------------------

    // implement ConverterRule
    public RelNode convert(RelNode rel)
    {
        ValuesRel valuesRel = (ValuesRel) rel;
        RelTraitSet traitSet = valuesRel.getTraits();
        FennelValuesRel fennelRel =
            new FennelValuesRel(
                valuesRel.getCluster(),
                valuesRel.getRowType(),
                valuesRel.getTuples());
        // copy over the other traits
        for (int i = 0; i < traitSet.size(); i++) {
            RelTrait trait = traitSet.getTrait(i);
            if (trait.getTraitDef() != getTraitDef()) {
                if (fennelRel.getTraits().getTrait(trait.getTraitDef())
                    != null)
                {
                    fennelRel.getTraits().setTrait(trait.getTraitDef(), trait);
                } else {
                    fennelRel.getTraits().addTrait(trait);
                }
            }
        }
        return fennelRel;
    }
}

// End FennelValuesRule.java
