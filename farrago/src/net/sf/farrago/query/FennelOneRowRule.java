/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

import java.util.*;
import java.math.*;

/**
 * FennelOneRowRule provides an implementation for {@link OneRowRel}
 * in terms of {@link FennelValuesRel}.
 *
 * @author Wael Chatila
 * @since Feb 4, 2005
 * @version $Id$
 */
public class FennelOneRowRule extends RelOptRule {

    public FennelOneRowRule()
    {
        super(new RelOptRuleOperand(OneRowRel.class, null));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call)
    {
        OneRowRel oneRowRel = (OneRowRel) call.rels[0];

        RexBuilder rexBuilder = oneRowRel.getCluster().getRexBuilder();
        RexLiteral literalZero = rexBuilder.makeExactLiteral(
            new BigDecimal(0));

        List<List<RexLiteral>> tuples =
            Collections.singletonList(
                Collections.singletonList(
                    literalZero));

        RelDataType rowType =
            OneRowRel.deriveOneRowType(oneRowRel.getCluster().getTypeFactory());

        FennelValuesRel valuesRel =
            new FennelValuesRel(oneRowRel.getCluster(), rowType, tuples);
        call.transformTo(valuesRel);
    }
}
