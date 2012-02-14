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

import java.math.*;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FennelOneRowRule provides an implementation for {@link OneRowRel} in terms of
 * {@link FennelValuesRel}.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Feb 4, 2005
 */
public class FennelOneRowRule
    extends ConverterRule
{
    public static final FennelOneRowRule instance =
        new FennelOneRowRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelOneRowRule.
     */
    private FennelOneRowRule()
    {
        super(
            OneRowRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelOneRowRule");
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode convert(RelNode rel)
    {
        OneRowRel oneRowRel = (OneRowRel) rel;

        RexBuilder rexBuilder = oneRowRel.getCluster().getRexBuilder();
        RexLiteral literalZero = rexBuilder.makeExactLiteral(new BigDecimal(0));

        List<List<RexLiteral>> tuples =
            Collections.singletonList(
                Collections.singletonList(
                    literalZero));

        RelDataType rowType =
            OneRowRel.deriveOneRowType(oneRowRel.getCluster().getTypeFactory());

        FennelValuesRel valuesRel =
            new FennelValuesRel(
                oneRowRel.getCluster(),
                rowType,
                tuples);
        return valuesRel;
    }
}

// End FennelOneRowRule.java
