/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
