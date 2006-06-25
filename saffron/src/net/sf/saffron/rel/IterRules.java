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

package net.sf.saffron.rel;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.oj.rel.IterOneRowRel;
import org.eigenbase.oj.rel.IterCalcRel;
import org.eigenbase.oj.rel.JavaRelImplementor;


/**
 * A collection of optimizer rules related to the  {@link
 * CallingConvention#ITERATOR iterator calling convention}.
 *
 * @version $Id$
 */
public abstract class IterRules
{
    //~ Inner Classes ---------------------------------------------------------

    /**
     * Rule to convert a {@link UnionRel} to {@link
     * CallingConvention#ITERATOR iterator calling convention}.
     */
    public static class UnionToIteratorRule extends ConverterRule
    {
        public UnionToIteratorRule()
        {
            this("UnionToIteratorRule");
        }

        protected UnionToIteratorRule(String description)
        {
            super(UnionRel.class, CallingConvention.NONE,
                CallingConvention.ITERATOR, description);
        }

        // factory method
        protected RelNode newIterConcatenateRel(RelOptCluster cluster, RelNode[] inputs)
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
                        union.getTraits(), CallingConvention.ITERATOR,
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
     * Refinement of {@link UnionToIteratorRule} which only applies to a
     * {@link UnionRel} all of whose input rows are the same type as its output
     * row. Luckily, a {@link CoerceInputsRule} will have made that happen.
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

    public static class OneRowToIteratorRule extends ConverterRule
    {
        public OneRowToIteratorRule()
        {
            super(OneRowRel.class, CallingConvention.NONE,
                CallingConvention.ITERATOR, "OneRowToIteratorRule");
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
    public static class IterCalcRule extends ConverterRule
    {
        public static final IterCalcRule instance = new IterCalcRule();

        private IterCalcRule()
        {
            super(CalcRel.class, CallingConvention.NONE,
                CallingConvention.ITERATOR, "IterCalcRule");
        }

        public RelNode convert(RelNode rel)
        {
            final CalcRel calc = (CalcRel) rel;
            final RelNode convertedChild =
                mergeTraitsAndConvert(
                    calc.getTraits(), CallingConvention.ITERATOR,
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
                convertedChild, calc.getProgram())) {
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

    /**
     * Rule to convert a {@link ProjectRel} to an {@link IterCalcRel}.
     */
    public static class ProjectToIteratorRule extends ConverterRule
    {
        public static final ProjectToIteratorRule instance =
            new ProjectToIteratorRule();

        private ProjectToIteratorRule()
        {
            super(ProjectRel.class, CallingConvention.NONE,
                CallingConvention.ITERATOR, "ProjectToIteratorRule");
        }

        public RelNode convert(RelNode rel)
        {
            final ProjectRel project = (ProjectRel) rel;
            RelNode inputRel = project.getChild();
            final RelNode iterChild =
                mergeTraitsAndConvert(
                    project.getTraits(), CallingConvention.ITERATOR, inputRel);
            if (iterChild == null) {
                return null;
            }

            final RexNode [] exps = project.getProjectExps();
            RexProgram program =
                RexProgram.create(
                    iterChild.getRowType(), exps, null, project.getRowType(),
                    project.getCluster().getRexBuilder());

             // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(program)) {
                return null;
            }

            // REVIEW: want to move canTranslate into RelImplementor
            // and implement it for Java & C++ calcs.
            final JavaRelImplementor relImplementor =
                project.getCluster().getPlanner().getJavaRelImplementor(project);
            if (!relImplementor.canTranslate(iterChild, program)) {
                // Some of the expressions cannot be translated into Java
                return null;
            }

            return new IterCalcRel(
                project.getCluster(),
                iterChild,
                program,
                project.getFlags());
        }
    }

    /**
     * Rule to convert a {@link ProjectRel} on top of a {@link FilterRel} to an
     * {@link IterCalcRel}.
     */
    public static class ProjectedFilterToIteratorRule extends RelOptRule
    {
        public static final ProjectedFilterToIteratorRule instance =
            new ProjectedFilterToIteratorRule();

        private ProjectedFilterToIteratorRule()
        {
            super(new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(FilterRel.class, null)
                    }));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        public void onMatch(RelOptRuleCall call)
        {
            ProjectRel project = (ProjectRel) call.rels[0];
            FilterRel filterRel = (FilterRel) call.rels[1];

            RelNode inputRel = filterRel.getChild();
            RexNode condition = filterRel.getCondition();

            RelNode iterChild =
                mergeTraitsAndConvert(
                    project.getTraits(), CallingConvention.ITERATOR, inputRel);

            if (iterChild == null) {
                return;
            }

            // Create a program containing the expressions from the project
            // and the filter expression.
            final RexNode [] exps = project.getProjectExps();
            final RelDataTypeField[] fields = project.getRowType().getFields();
            final RelDataType inputRowType = inputRel.getRowType();
            final RexProgramBuilder programBuilder = new RexProgramBuilder(
                inputRowType, inputRel.getCluster().getRexBuilder());
            for (int i = 0; i < exps.length; i++) {
                programBuilder.addProject(exps[i], fields[i].getName());
            }
            if (condition != null) {
                programBuilder.addCondition(condition);
            }
            final RexProgram program = programBuilder.getProgram();

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(program)) {
                return;
            }

            // REVIEW: want to move canTranslate into RelImplementor
            // and implement it for Java & C++ calcs.
            final JavaRelImplementor relImplementor =
                call.getPlanner().getJavaRelImplementor(project);
            if (!relImplementor.canTranslate(iterChild, program)) {
                // some of the expressions cannot be translated into Java
                return;
            }
            IterCalcRel calcRel =
                new IterCalcRel(
                    project.getCluster(),
                    iterChild,
                    program,
                    project.getFlags());

            call.transformTo(calcRel);
        }
    }
}


// End IterRules.java
