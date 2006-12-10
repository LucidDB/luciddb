/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj;

import com.disruptivetech.farrago.volcano.*;

import net.sf.saffron.core.ImplementableTable;
import net.sf.saffron.oj.convert.*;
import net.sf.saffron.oj.rel.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.convert.FactoryConverterRule;
import org.eigenbase.rel.jdbc.JdbcQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;

/**
 * OJPlannerFactory constructs planners initialized to handle all calling
 * conventions, rules, and relational expressions needed to preprocess Saffron
 * extended Java.
 *
 * @version $Id$
 */
public class OJPlannerFactory
{
    private static ThreadLocal<OJPlannerFactory> threadInstances =
        new ThreadLocal<OJPlannerFactory>();

    //~ Methods ---------------------------------------------------------------

    public static void setThreadInstance(OJPlannerFactory plannerFactory)
    {
        threadInstances.set(plannerFactory);
    }

    public static OJPlannerFactory threadInstance()
    {
        return threadInstances.get();
    }

    public RelOptPlanner newPlanner()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        // Create converter rules for all of the standard calling conventions.
        add(
            planner,
            new CollectionToArrayConvertlet());
        add(
            planner,
            new VectorToArrayConvertlet());
        add(
            planner,
            new ArrayToJavaConvertlet());
        add(
            planner,
            new IteratorToJavaConvertlet());
        add(
            planner,
            new EnumerationToJavaConvertlet());
        add(
            planner,
            new VectorToJavaConvertlet());
        add(
            planner,
            new MapToJavaConvertlet());
        add(
            planner,
            new HashtableToJavaConvertlet());
        add(
            planner,
            new IterableToJavaConvertlet());
        add(
            planner,
            new ResultSetToJavaConvertlet());
        add(
            planner,
            new JavaToCollectionConvertlet());
        add(
            planner,
            new VectorToEnumerationConvertlet());
        add(
            planner,
            new JavaToExistsConvertlet());
        add(
            planner,
            new ArrayToExistsConvertlet());
        add(
            planner,
            new IteratorToExistsConvertlet());
        add(
            planner,
            new MapToExistsConvertlet());
        add(
            planner,
            new HashtableToExistsConvertlet());
        add(
            planner,
            new EnumerationToExistsConvertlet());
        add(
            planner,
            new CollectionToExistsConvertlet());
        add(
            planner,
            new JavaToIterableConvertlet());
        add(
            planner,
            new IteratorToIterableConvertlet());
        add(
            planner,
            new CollectionToIterableConvertlet());
        add(
            planner,
            new VectorToIterableConvertlet());
        add(
            planner,
            new IterableToIteratorConvertlet());
        add(
            planner,
            new CollectionToIteratorConvertlet());
        add(
            planner,
            new VectorToIteratorConvertlet());
        add(
            planner,
            new EnumerationToIteratorConvertlet());
        add(
            planner,
            new ResultSetToIteratorConvertlet());
        add(
            planner,
            new IteratorToResultSetConvertlet());
        add(
            planner,
            new JavaToVectorConvertlet());

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
        RelOptUtil.registerAbstractRels(planner);
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
        planner.addRule(new IterRules.UnionToIteratorRule());
        planner.addRule(new IterRules.OneRowToIteratorRule());
    }

    /**
     * Wraps a convertlet in a {@link ConverterRule}, then registers it with a
     * planner.
     */
    private static void add(
        RelOptPlanner planner,
        JavaConvertlet convertlet)
    {
        planner.addRule(new FactoryConverterRule(convertlet));
    }

    public static class AggregateToJavaRule extends ConverterRule
    {
        public AggregateToJavaRule()
        {
            super(AggregateRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "AggregateToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final AggregateRel aggregate = (AggregateRel) rel;
            final RelNode javaChild =
                mergeTraitsAndConvert(
                    aggregate.getTraits(), CallingConvention.JAVA,
                    aggregate.getChild());
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
    public static class DistinctToExistsRule extends RelOptRule
    {
        public DistinctToExistsRule()
        {
            super(new RelOptRuleOperand(JavaDistinctRel.class, null));
        }

        public void onMatch(RelOptRuleCall call)
        {
            JavaDistinctRel distinct = (JavaDistinctRel) call.rels[0];
            RelDataType rowType = distinct.getChild().getRowType();
            if (rowType.getFieldList().size() == 0) {
                call.transformTo(
                    new JavaExistsRel(
                        distinct.getCluster(),
                        distinct.getChild()));
            }
        }
    }

    public static class DistinctToJavaRule extends ConverterRule
    {
        public DistinctToJavaRule()
        {
            super(AggregateRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "DistinctToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final AggregateRel distinct = (AggregateRel) rel;
            if (!distinct.isDistinct()) {
                return null;
            }
            final RelNode javaChild =
                mergeTraitsAndConvert(
                    distinct.getTraits(), CallingConvention.JAVA,
                    distinct.getChild());
            if (javaChild == null) {
                return null;
            }
            return new JavaDistinctRel(
                distinct.getCluster(),
                javaChild);
        }
    }

    public static class FilterToJavaRule extends ConverterRule
    {
        public FilterToJavaRule()
        {
            super(FilterRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "FilterToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final FilterRel filter = (FilterRel) rel;
            final RelNode javaChild =
                mergeTraitsAndConvert(
                    filter.getTraits(), CallingConvention.JAVA,
                    filter.getChild());
            if (javaChild == null) {
                return null;
            }
            return new JavaFilterRel(
                filter.getCluster(),
                javaChild,
                filter.getCondition());
        }
    }

    public static class JoinToJavaRule extends ConverterRule
    {
        public JoinToJavaRule()
        {
            super(JoinRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "JoinToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final JoinRel join = (JoinRel) rel;
            CallingConvention convention = CallingConvention.JAVA;
            String [] variables =
                RelOptUtil.getVariablesSetAndUsed(
                    join.getRight(),
                    join.getLeft());
            if (variables.length > 0) {
                // Can't implement if left is dependent upon right (don't worry,
                // we'll be called with left and right swapped, if that's
                // possible)
                return null;
            }
            final RelNode convertedLeft = mergeTraitsAndConvert(
                    join.getTraits(),
                    convention,
                    join.getLeft());
            if (convertedLeft == null) {
                return null;
            }
            final RelNode convertedRight =
                mergeTraitsAndConvert(
                    join.getTraits(),
                    convention,
                    join.getRight());
            if (convertedRight == null) {
                return null;
            }
            return new JavaNestedLoopJoinRel(
                join.getCluster(),
                convertedLeft,
                convertedRight,
                join.getCondition().clone(),
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
            super(OneRowRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "OneRowToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            // REVIEW: SWZ: 3/5/2005: Might need to propagate other 
            // traits fro OneRowRel to JavaOneRowRel
            final OneRowRel oneRow = (OneRowRel) rel;
            return new JavaOneRowRel(oneRow.getCluster());
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
            super(ProjectRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "ProjectToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final ProjectRel project = (ProjectRel) rel;
            final RelNode javaChild =
                mergeTraitsAndConvert(
                    project.getTraits(), CallingConvention.JAVA,
                    project.getChild());
            if (javaChild == null) {
                return null;
            }
            return new JavaProjectRel(
                project.getCluster(),
                javaChild,
                project.getProjectExps(),
                project.getRowType(),
                project.getFlags());
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
            super(TableAccessRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "TableAccessToJavaRule");
        }

        public RelNode convert(RelNode rel)
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
     * Rule to converts a {@link UnionRel} to {@link CallingConvention#JAVA
     * Java calling convention}.
     */
    public static class UnionToJavaRule extends ConverterRule
    {
        public UnionToJavaRule()
        {
            super(UnionRel.class, CallingConvention.NONE,
                CallingConvention.JAVA, "UnionToJavaRule");
        }

        public RelNode convert(RelNode rel)
        {
            final UnionRel union = (UnionRel) rel;
            if (union.isDistinct()) {
                return null; // can only convert non-distinct Union
            }
            RelNode [] newInputs = new RelNode[union.getInputs().length];
            for (int i = 0; i < newInputs.length; i++) {
                newInputs[i] =
                    mergeTraitsAndConvert(
                        union.getTraits(), CallingConvention.JAVA,
                        union.getInputs()[i]);
                if (newInputs[i] == null) {
                    return null; // cannot convert this input
                }
            }
            return new JavaUnionAllRel(
                union.getCluster(),
                newInputs);
        }
    }
}


// End OJPlannerFactory.java
