/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * FennelEmptyRule provides an implementation for {@link
 * org.eigenbase.rel.EmptyRel} in terms of {@link
 * net.sf.farrago.fennel.rel.FennelValuesRel}.
 *
 * @author jhyde
 * @version $Id$
 */
public class FennelEmptyRule
    extends ConverterRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Singleton instance of this rule.
     */
    public static final FennelEmptyRule instance = new FennelEmptyRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelEmptyRule.
     */
    private FennelEmptyRule()
    {
        super(
            EmptyRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelEmptyRule");
    }

    //~ Methods ----------------------------------------------------------------

    // implement ConverterRule
    public RelNode convert(RelNode rel)
    {
        EmptyRel valuesRel = (EmptyRel) rel;

        return new FennelValuesRel(
            valuesRel.getCluster(),
            valuesRel.getRowType(),
            Collections.<List<RexLiteral>>emptyList());
    }
}

// End FennelEmptyRule.java
