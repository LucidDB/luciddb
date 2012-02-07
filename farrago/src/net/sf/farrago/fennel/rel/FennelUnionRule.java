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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;


/**
 * Rule to convert a {@link UnionRel} to {@link FennelRel#FENNEL_EXEC_CONVENTION
 * Fennel calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelUnionRule
    extends ConverterRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FennelUnionRule instance = new FennelUnionRule();

    //~ Constructors -----------------------------------------------------------

    public FennelUnionRule()
    {
        super(
            UnionRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelUnionRule");
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode convert(RelNode rel)
    {
        final UnionRel unionRel = (UnionRel) rel;
        if (!unionRel.isHomogeneous()) {
            // Fennel's MergeExecStream only operates on homogeneous inputs;
            // we'll try again once {@link CoerceInputsRule} has taken
            // care of that.
            return null;
        }
        if (unionRel.isDistinct()) {
            // can only convert non-distinct Union; we'll try again once {@link
            // UnionToDistinctRule} and {@link FennelDistinctSortRule} have
            // taken care of that.
            return null;
        }
        RelNode [] newInputs = new RelNode[unionRel.getInputs().length];
        for (int i = 0; i < newInputs.length; i++) {
            newInputs[i] =
                mergeTraitsAndConvert(
                    unionRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    unionRel.getInput(i));
            if (newInputs[i] == null) {
                return null; // cannot convert this input
            }
        }
        return new FennelMergeRel(
            unionRel.getCluster(),
            newInputs);
    }
}

// End FennelUnionRule.java
