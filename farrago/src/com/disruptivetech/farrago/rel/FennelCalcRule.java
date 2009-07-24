/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.disruptivetech.farrago.rel;

import com.disruptivetech.farrago.calc.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * FennelCalcRule is a rule for implementing {@link CalcRel} via a Fennel
 * Calculator ({@link FennelCalcRel}).
 *
 * <p/>
 *
 * @author jhyde
 * @version $Id$
 */
public class FennelCalcRule
    extends ConverterRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FennelCalcRule instance = new FennelCalcRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelCalcRule object.
     */
    protected FennelCalcRule()
    {
        super(
            CalcRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelCalcRule");
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    public RelNode convert(RelNode rel)
    {
        CalcRel calc = (CalcRel) rel;
        RelNode relInput = rel.getInput(0);
        RelNode fennelInput =
            mergeTraitsAndConvert(
                calc.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return null;
        }

        // If there's a multiset, let FarragoMultisetSplitter work on it first.
        if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
            return null;
        }

        final RexToCalcTranslator translator =
            new RexToCalcTranslator(
                calc.getCluster().getRexBuilder(),
                calc);
        if (!translator.canTranslate(calc.getProgram())) {
            return null;
        }

        return newFennelCalcRel(calc, fennelInput);
    }

    /**
     * Factory method called from {@link #convert}.
     * A specialized subclass of {@link FennelCalcRel} likely corresponds to a
     * subclass of FennelCalcRule. The subclass of the rule can inherit {@link
     * #convert} (just above), but overide this factory method to produce the
     * subclass of the FennelCalRel.
     */
    protected FennelCalcRel newFennelCalcRel(CalcRel calc, RelNode fennelInput)
    {
        return new FennelCalcRel(
            calc.getCluster(),
            fennelInput,
            calc.getRowType(),
            calc.getProgram());
    }
}

// End FennelCalcRule.java
