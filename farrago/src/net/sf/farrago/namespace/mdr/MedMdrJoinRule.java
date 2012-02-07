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
package net.sf.farrago.namespace.mdr;

import java.util.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * MedMdrJoinRule is a rule for converting a JoinRel into a MedMdrJoinRel when
 * the join condition navigates an association.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    MedMdrJoinRule()
    {
        // TODO:  allow join to work on inputs other
        // than MedMdrClassExtentRel (e.g. filters, projects, other joins)
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(MedMdrClassExtentRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        RelNode leftRel = call.rels[1];
        MedMdrClassExtentRel rightRel = (MedMdrClassExtentRel) call.rels[2];

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        if ((joinRel.getJoinType() != JoinRelType.INNER)
            && (joinRel.getJoinType() != JoinRelType.LEFT))
        {
            return;
        }

        int [] joinFieldOrdinals = new int[2];
        if (!RelOptUtil.analyzeSimpleEquiJoin(joinRel, joinFieldOrdinals)) {
            return;
        }
        int leftOrdinal = joinFieldOrdinals[0];
        int rightOrdinal = joinFieldOrdinals[1];

        // on right side, must join to reference field which refers to
        // left side type
        List<StructuralFeature> features =
            JmiObjUtil.getFeatures(
                rightRel.mdrClassExtent.refClass,
                StructuralFeature.class,
                false);
        Reference reference;
        if (rightOrdinal == features.size()) {
            // join to mofId: this is a many-to-one join (primary key lookup on
            // right hand side), which we will represent with a null reference
            reference = null;
        } else {
            if (rightOrdinal > features.size()) {
                // Pseudocolumn such as mofClassName:  can't join.
                return;
            }
            StructuralFeature feature = features.get(rightOrdinal);
            if (!(feature instanceof Reference)) {
                return;
            }
            reference = (Reference) feature;
        }

        // TODO:  verify that leftOrdinal specifies a MOFID of an
        // appropriate type; also, verify that left and right
        // are from same repository

        /*
        Classifier referencedType = reference.getReferencedEnd().getType();
         Classifier leftType = (Classifier)
         leftRel.mdrClassExtent.refClass.refMetaObject(); if
         (!leftType.equals(referencedType) &&
         !leftType.allSupertypes().contains(referencedType)) { // REVIEW: we now
         know this is a bogus join; could optimize it by // skipping querying
         altogether, but a warning of some kind would // be friendlier return; }
         */
        RelNode iterLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                CallingConvention.ITERATOR,
                leftRel);
        if (iterLeft == null) {
            return;
        }

        RelNode iterRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                CallingConvention.ITERATOR,
                rightRel);
        if (iterRight == null) {
            return;
        }

        call.transformTo(
            new MedMdrJoinRel(
                joinRel.getCluster(),
                iterLeft,
                iterRight,
                joinRel.getCondition(),
                joinRel.getJoinType(),
                leftOrdinal,
                reference));
    }
}

// End MedMdrJoinRule.java
