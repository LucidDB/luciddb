/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.oj;

import net.sf.saffron.core.ImplementableTable;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.convert.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.ConverterRule;
import net.sf.saffron.rel.convert.NoneConverterRel;
import net.sf.saffron.rel.jdbc.JdbcQuery;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;

/**
 * OJPlannerFactory implements VolcanoPlannerFactory by constructing planners
 * initialized to handle all calling conventions, rules, and relational
 * expressions needed to preprocess Saffron extended Java.
 *
 * @version $Id$
 */
public class OJPlannerFactory extends VolcanoPlannerFactory
{
    //~ Methods ---------------------------------------------------------------

    public VolcanoPlanner newPlanner()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        // Registers the standard calling conventions.
        for (int i = 0; i < CallingConvention.values.length; i++) {
            planner.addCallingConvention(CallingConvention.values[i]);
        }

        // Create converter rules for all of the standard calling conventions.
        ArrayConverterRel.init(planner);
        CollectionConverterRel.init(planner);
        EnumerationConverterRel.init(planner);
        IterableConverterRel.init(planner);
        IterConverterRel.init(planner);
        JavaConverterRel.init(planner);
        NoneConverterRel.init(planner);
        ResultSetConverterRel.init(planner);
        VectorConverterRel.init(planner);
        ExistsConverterRel.init(planner);

        planner.registerAbstractRelationalRules();

        // custom rules
        // REVIEW jvs 24-Sept-2003:  These were commented out in
        // VolcanoPlanner.
        /*
          addRule(new ExpressionReader.MapToHashtableReaderRule());
          addRule(new Exp.IteratorExpressionRule()); // todo: enable
          addRule(new Exp.PlanExpressionRule()); // todo: enable
          addRule(new Query.TranslateRule());
          addRule(new In.InSemiJoinRule()); // todo: enable
          addRule(new RelWrapper.RemoveWrapperRule()); // todo: enable
        */
        planner.registerAbstractRels();
        registerJavaRels(planner);
        registerIterRels(planner);

        // custom rels
        JdbcQuery.register(planner);

        return planner;
    }

    /**
     * Register all of the Java implementations for the abstract relational
     * operators, along with appropriate conversion rules.
     *
     * @param planner the planner in which to register the rules
     */
    public static void registerJavaRels(VolcanoPlanner planner)
    {
        planner.addRule(new AggregateToJavaRule());
        planner.addRule(new FilterToJavaRule());
        planner.addRule(new OneRowToJavaRule());
        planner.addRule(new DistinctToJavaRule());
        planner.addRule(new DistinctToExistsRule());
        planner.addRule(new JoinToJavaRule());
        planner.addRule(new ProjectToJavaRule());
        planner.addRule(new TableAccessToJavaRule());
        planner.addRule(new UnionToJavaRule());
    }

    public static void registerIterRels(VolcanoPlanner planner)
    {
        planner.addRule(new UnionToIteratorRule());
        planner.addRule(new OneRowToIteratorRule());
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class AggregateToJavaRule extends ConverterRule
    {
        public AggregateToJavaRule()
        {
            super(
                AggregateRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "AggregateToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final AggregateRel aggregate = (AggregateRel) rel;
            final SaffronRel javaChild =
                convert(planner,aggregate.child,CallingConvention.JAVA);
            if (javaChild == null) {
                return null;
            }
            return new JavaAggregateRel(
                aggregate.getCluster(),
                javaChild,
                aggregate.getGroupCount(),
                aggregate.getAggCalls());
        }
    }

    /**
     * Rule to translate a {@link net.sf.saffron.oj.rel.JavaDistinctRel} into a
     * {@link net.sf.saffron.oj.rel.JavaExistsRel}, provided that the select
     * list contains zero columns.
     */
    public static class DistinctToExistsRule extends VolcanoRule
    {
        public DistinctToExistsRule()
        {
            super(new RuleOperand(JavaDistinctRel.class,null));
        }

        public void onMatch(VolcanoRuleCall call)
        {
            JavaDistinctRel distinct = (JavaDistinctRel) call.rels[0];
            SaffronType rowType = distinct.child.getRowType();
            if (rowType.getFieldCount() == 0) {
                call.transformTo(
                    new JavaExistsRel(distinct.getCluster(),distinct.child));
            }
        }
    }

    public static class DistinctToJavaRule extends ConverterRule
    {
        public DistinctToJavaRule()
        {
            super(
                DistinctRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "DistinctToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final DistinctRel distinct = (DistinctRel) rel;
            final SaffronRel javaChild =
                convert(planner,distinct.child,CallingConvention.JAVA);
            if (javaChild == null) {
                return null;
            }
            return new JavaDistinctRel(distinct.getCluster(),javaChild);
        }
    }

    public static class FilterToJavaRule extends ConverterRule
    {
        public FilterToJavaRule()
        {
            super(
                FilterRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "FilterToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final FilterRel filter = (FilterRel) rel;
            final SaffronRel javaChild =
                convert(planner,filter.child,CallingConvention.JAVA);
            if (javaChild == null) {
                return null;
            }
            return new JavaFilterRel(
                filter.getCluster(),
                javaChild,
                filter.condition);
        }
    }

    public static class JoinToJavaRule extends ConverterRule
    {
        public JoinToJavaRule()
        {
            super(
                JoinRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "JoinToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final JoinRel join = (JoinRel) rel;
            CallingConvention convention = CallingConvention.JAVA;
            String [] variables =
                OptUtil.getVariablesSetAndUsed(join.getRight(),join.getLeft());
            if (variables.length > 0) {
                // Can't implement if left is dependent upon right (don't worry,
                // we'll be called with left and right swapped, if that's
                // possible)
                return null;
            }
            final SaffronRel convertedLeft =
                convert(planner,join.getLeft(),convention);
            if (convertedLeft == null) {
                return null;
            }
            final SaffronRel convertedRight =
                convert(planner,join.getRight(),convention);
            if (convertedRight == null) {
                return null;
            }
            return new JavaNestedLoopJoinRel(
                join.getCluster(),
                convertedLeft,
                convertedRight,
                RexUtil.clone(join.getCondition()),
                join.getJoinType(),
                join.getVariablesStopped());
        }
    }

    /**
     * Converts a {@link OneRowRel} to {@link CallingConvention#JAVA Java
     * calling convention}.
     */
    public static class OneRowToJavaRule extends ConverterRule
    {
        public OneRowToJavaRule()
        {
            super(
                OneRowRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "OneRowToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final OneRowRel oneRow = (OneRowRel) rel;
            return new JavaOneRowRel(oneRow.getCluster());
        }
    }

    public static class OneRowToIteratorRule extends ConverterRule
    {
        public OneRowToIteratorRule()
        {
            super(
                OneRowRel.class,
                CallingConvention.NONE,
                CallingConvention.ITERATOR,
                "OneRowToIteratorRule");
        }
        
        public SaffronRel convert(SaffronRel rel)
        {
            final OneRowRel oneRow = (OneRowRel) rel;
            return new IterOneRowRel(oneRow.getCluster());
        }
    }

    /**
     * Converts a {@link ProjectRel} to {@link CallingConvention#JAVA Java
     * calling convention}.
     */
    public static class ProjectToJavaRule extends ConverterRule
    {
        public ProjectToJavaRule()
        {
            super(
                ProjectRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "ProjectToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final ProjectRel project = (ProjectRel) rel;
            if (project instanceof JavaProjectRel) {
                return null;
            }
            final SaffronRel javaChild =
                convert(planner,project.child,CallingConvention.JAVA);
            if (javaChild == null) {
                return null;
            }
            return new JavaProjectRel(
                project.getCluster(),
                javaChild,
                project.getChildExps(),
                project.getFieldNames(),
                project.getFlags());
        }
    }

    /**
     * Rule to convert a {@link ProjectRel} to an {@link IterCalcRel}.
     */
    public static class ProjectToIteratorRule extends ConverterRule
    {
        private ProjectToIteratorRule() {
            super(
                    ProjectRel.class,
                    CallingConvention.NONE,
                    CallingConvention.ITERATOR,
                    "ProjectToIteratorRule");
        }
        public static ProjectToIteratorRule instance =
                new ProjectToIteratorRule();

        public SaffronRel convert(SaffronRel rel)
        {
            final ProjectRel project = (ProjectRel) rel;
            SaffronRel inputRel = project.child;
            final SaffronRel iterChild = convert(
                planner,inputRel,CallingConvention.ITERATOR);
            if (iterChild == null) {
                return null;
            }
            final RexNode [] exps = project.getChildExps();
            final RexNode condition = null;
            // FIXME jvs 11-May-2004: This should be calling something
            // (cluster?) to get an existing RelImplementor, not making one up
            // out of thin air, since query processing may be using a custom
            // implementation.
            final RelImplementor relImplementor =
                    new RelImplementor(project.getCluster().rexBuilder);
            if (!relImplementor.canTranslate(project, condition, exps)) {
                // some of the expressions cannot be translated into Java
                return null;
            }
            return new IterCalcRel(
                project.getCluster(),
                iterChild,
                exps,
                condition,
                project.getFieldNames(),
                project.getFlags());
        }
    }

    /**
     * Rule to convert a {@link ProjectRel} on top of a {@link FilterRel} to an
     * {@link IterCalcRel}.
     */
    public static class ProjectedFilterToIteratorRule extends VolcanoRule
    {
        private ProjectedFilterToIteratorRule()
        {
            super(
                new RuleOperand(
                    ProjectRel.class,
                    new RuleOperand [] { new RuleOperand(FilterRel.class,null) }
                    ));
        }
        public static final ProjectedFilterToIteratorRule instance =
                new ProjectedFilterToIteratorRule();

        // implement VolcanoRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        public void onMatch(VolcanoRuleCall call)
        {
            ProjectRel project = (ProjectRel) call.rels[0];
            FilterRel filterRel = (FilterRel) call.rels[1];

            SaffronRel inputRel = filterRel.child;
            RexNode condition = filterRel.condition;
            
            SaffronRel iterChild = convert(
                planner,inputRel,CallingConvention.ITERATOR);
            
            if (iterChild == null) {
                return;
            }
            
            final RexNode[] exps = project.getChildExps();
            final RelImplementor relImplementor =
                    new RelImplementor(project.getCluster().rexBuilder);
            if (!relImplementor.canTranslate(project, condition, exps)) {
                // some of the expressions cannot be translated into Java
                return;
            }
            IterCalcRel calcRel = new IterCalcRel(
                project.getCluster(),
                iterChild,
                exps,
                condition,
                project.getFieldNames(),
                project.getFlags());

            call.transformTo(calcRel);
        }
    }
    
    /**
     * Rule to convert a {@link CalcRel} to an {@link IterCalcRel}.
     */
    public static class IterCalcRule extends ConverterRule
    {
        private IterCalcRule() {
            super(CalcRel.class, CallingConvention.NONE,
                    CallingConvention.ITERATOR, "IterCalcRule");
        }
        public static final IterCalcRule instance = new IterCalcRule();

        public SaffronRel convert(SaffronRel rel) {
            // TODO jvs 11-May-2004:  add canTranslate test
            final CalcRel calc = (CalcRel) rel;
            final SaffronRel convertedChild = convert(planner, calc.child,
                    CallingConvention.ITERATOR);
            if (convertedChild == null) {
                // We can't convert the child, so we can't convert rel.
                return null;
            }
            return new IterCalcRel(rel.getCluster(), convertedChild,
                    calc._projectExprs, calc._conditionExpr,
                    OptUtil.getFieldNames(calc.getRowType()),
                    IterCalcRel.Flags.Boxed);
        }
    }

    /**
     * Rule to converts a {@link TableAccessRel} to {@link
     * CallingConvention#JAVA Java calling convention}.
     */
    public static class TableAccessToJavaRule extends ConverterRule
    {
        public TableAccessToJavaRule()
        {
            super(
                TableAccessRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "TableAccessToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final TableAccessRel tableAccess = (TableAccessRel) rel;
            if (!(tableAccess.getTable() instanceof ImplementableTable)) {
                return null;
            }
            return new JavaTableAccessRel(
                tableAccess.getCluster(),
                (ImplementableTable) tableAccess.getTable(),
                tableAccess.getConnection());
        }
    }

    /**
     * Rule to converts a {@link UnionRel} to {@link
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
            super(
                UnionRel.class,
                CallingConvention.NONE,
                CallingConvention.ITERATOR,
                description);
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final UnionRel union = (UnionRel) rel;
            if (union.getClass() != UnionRel.class) {
                return null; // require precise class, otherwise we loop
            }
            if (union.isDistinct()) {
                return null; // can only convert non-distinct Union
            }
            SaffronRel [] newInputs = new SaffronRel[union.getInputs().length];
            for (int i = 0; i < newInputs.length; i++) {
                // Stubborn, because inputs don't appear as operands.
                newInputs[i] =
                    convert(
                        planner,
                        union.getInputs()[i],
                        CallingConvention.ITERATOR);
                if (newInputs[i] == null) {
                    return null; // cannot convert this input
                }
            }
            return new IterConcatenateRel(union.getCluster(),newInputs);
        }
    }

    public static class HomogeneousUnionToIteratorRule
        extends OJPlannerFactory.UnionToIteratorRule
    {
        public HomogeneousUnionToIteratorRule()
        {
            super("HomogeneousUnionToIteratorRule");
        }
        
        public SaffronRel convert(SaffronRel rel)
        {
            final UnionRel unionRel = (UnionRel) rel;
            SaffronType unionType = unionRel.getRowType();
            SaffronRel [] inputs = unionRel.getInputs();
            for (int i = 0; i < inputs.length; ++i) {
                SaffronType inputType = inputs[i].getRowType();
                if (!OptUtil.areRowTypesEqual(inputType,unionType)) {
                    return null;
                }
            }
            return super.convert(rel);
        }
    }

    /**
     * Rule to converts a {@link UnionRel} to {@link CallingConvention#JAVA
     * Java calling convention}.
     */
    public static class UnionToJavaRule extends ConverterRule
    {
        public UnionToJavaRule()
        {
            super(
                UnionRel.class,
                CallingConvention.NONE,
                CallingConvention.JAVA,
                "UnionToJavaRule");
        }

        public SaffronRel convert(SaffronRel rel)
        {
            final UnionRel union = (UnionRel) rel;
            if (union.getClass() != UnionRel.class) {
                return null; // require precise class, otherwise we loop
            }
            if (union.isDistinct()) {
                return null; // can only convert non-distinct Union
            }
            SaffronRel [] newInputs = new SaffronRel[union.getInputs().length];
            for (int i = 0; i < newInputs.length; i++) {
                newInputs[i] =
                    convert(
                        planner,
                        union.getInputs()[i],
                        CallingConvention.JAVA);
                if (newInputs[i] == null) {
                    return null; // cannot convert this input
                }
            }
            return new JavaUnionAllRel(union.getCluster(),newInputs);
        }
    }
}


// End OJPlannerFactory.java
