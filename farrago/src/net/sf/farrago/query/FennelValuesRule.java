/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.query;

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

    //~ Constructors -----------------------------------------------------------

    public FennelValuesRule()
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

        FennelValuesRel fennelRel =
            new FennelValuesRel(
                valuesRel.getCluster(),
                valuesRel.getRowType(),
                valuesRel.getTuples());
        return fennelRel;
    }
}

// End FennelValuesRule.java
