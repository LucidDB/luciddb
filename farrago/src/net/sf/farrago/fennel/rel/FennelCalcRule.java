/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.fennel.rel;

import net.sf.farrago.fennel.calc.*;

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
        this("FennelCalcRule");
    }

    /**
     * Creates a new FennelCalcRule object.
     */
    protected FennelCalcRule(String description)
    {
        super(
            CalcRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            description);
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

    /** FennelRel factory method */
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
