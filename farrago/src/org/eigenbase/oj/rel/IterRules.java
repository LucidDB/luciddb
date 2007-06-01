/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.oj.rel;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A collection of optimizer rules related to the {@link
 * CallingConvention#ITERATOR iterator calling convention}.
 *
 * @version $Id$
 */
public abstract class IterRules
{
    //~ Inner Classes ----------------------------------------------------------

    /**
     * Rule to convert a {@link UnionRel} to {@link CallingConvention#ITERATOR
     * iterator calling convention}.
     */
    public static class UnionToIteratorRule
        extends ConverterRule
    {
        public UnionToIteratorRule()
        {
            this("UnionToIteratorRule");
        }

        protected UnionToIteratorRule(String description)
        {
            super(
                UnionRel.class,
                CallingConvention.NONE,
                CallingConvention.ITERATOR,
                description);
        }

        // factory method
        protected RelNode newIterConcatenateRel(
            RelOptCluster cluster,
            RelNode [] inputs)
        {
            return new IterConcatenateRel(cluster, inputs);
        }

        public RelNode convert(RelNode rel)
        {
            final UnionRel union = (UnionRel) rel;
            if (union.isDistinct()) {
                return null; // can only convert non-distinct Union
            }
            RelNode [] newInputs = new RelNode[union.getInputs().length];
            for (int i = 0; i < newInputs.length; i++) {
                // Stubborn, because inputs don't appear as operands.
                newInputs[i] =
                    mergeTraitsAndConvert(
                        union.getTraits(),
                        CallingConvention.ITERATOR,
                        union.getInput(i));
                if (newInputs[i] == null) {
                    return null; // cannot convert this input
                }
            }
            return newIterConcatenateRel(
                union.getCluster(),
                newInputs);
        }
    }

    /**
     * Refinement of {@link UnionToIteratorRule} which only applies to a {@link
     * UnionRel} all of whose input rows are the same type as its output row.
     * Luckily, a {@link CoerceInputsRule} will have made that happen.
     */
    public static class HomogeneousUnionToIteratorRule
        extends UnionToIteratorRule
    {
        public HomogeneousUnionToIteratorRule()
        {
            this("HomogeneousUnionToIteratorRule");
        }

        protected HomogeneousUnionToIteratorRule(String description)
        {
            super(description);
        }

        public RelNode convert(RelNode rel)
        {
            final UnionRel unionRel = (UnionRel) rel;
            if (!unionRel.isHomogeneous()) {
                return null;
            }
            return super.convert(rel);
        }
    }

    public static class OneRowToIteratorRule
        extends ConverterRule
    {
        public OneRowToIteratorRule()
        {
            super(
                OneRowRel.class,
                CallingConvention.NONE,
                CallingConvention.ITERATOR,
                "OneRowToIteratorRule");
        }

        public RelNode convert(RelNode rel)
        {
            final OneRowRel oneRow = (OneRowRel) rel;
            return new IterOneRowRel(oneRow.getCluster());
        }
    }

    /**
     * Rule to convert a {@link CalcRel} to an {@link IterCalcRel}.
     */
    public static class IterCalcRule
        extends ConverterRule
    {
        public static final IterCalcRule instance = new IterCalcRule();

        private IterCalcRule()
        {
            super(
                CalcRel.class,
                CallingConvention.NONE,
                CallingConvention.ITERATOR,
                "IterCalcRule");
        }

        public RelNode convert(RelNode rel)
        {
            final CalcRel calc = (CalcRel) rel;
            final RelNode convertedChild =
                mergeTraitsAndConvert(
                    calc.getTraits(),
                    CallingConvention.ITERATOR,
                    calc.getChild());
            if (convertedChild == null) {
                // We can't convert the child, so we can't convert rel.
                return null;
            }

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
                return null;
            }

            // REVIEW: want to move canTranslate into RelImplementor
            // and implement it for Java & C++ calcs.
            final JavaRelImplementor relImplementor =
                rel.getCluster().getPlanner().getJavaRelImplementor(rel);
            if (!relImplementor.canTranslate(
                    convertedChild,
                    calc.getProgram()))
            {
                // Some of the expressions cannot be translated into Java
                return null;
            }

            return new IterCalcRel(
                rel.getCluster(),
                convertedChild,
                calc.getProgram(),
                ProjectRelBase.Flags.Boxed);
        }
    }
}

// End IterRules.java
