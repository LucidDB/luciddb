/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.*;


/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of {@link
 * ProjectRel} and {@link FilterRel}. It should be created in the latter stages
 * of optimization, by merging consecutive {@link ProjectRel} and {@link
 * FilterRel} nodes together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a {@link
 * ProjectRel}</li>
 * <li>{@link MergeCalcRule} merges two CalcRels</li>
 * </ul>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 7, 2004
 */
public final class CalcRel
    extends CalcRelBase
{
    //~ Static fields/initializers ---------------------------------------------

    public static final boolean DeprecateProjectAndFilter = false;

    //~ Constructors -----------------------------------------------------------

    public CalcRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        RexProgram program,
        List<RelCollation> collationList)
    {
        super(cluster, traits, child, rowType, program, collationList);
    }

    //~ Methods ----------------------------------------------------------------

    public CalcRel clone()
    {
        return new CalcRel(
            getCluster(),
            cloneTraits(),
            getChild(),
            rowType,
            program.copy(),
            getCollationList());
    }

    /**
     * Creates a relational expression which projects a set of expressions.
     *
     * @param child input relational expression
     * @param exprList list of expressions for the input columns
     * @param fieldNameList aliases of the expressions, or null to generate
     */
    public static RelNode createProject(
        RelNode child,
        List<RexNode> exprList,
        List<String> fieldNameList)
    {
        RexNode [] exprs = exprList.toArray(new RexNode[exprList.size()]);
        String [] fieldNames =
            (fieldNameList == null) ? null
            : fieldNameList.toArray(new String[fieldNameList.size()]);
        return CalcRel.createProject(
            child,
            exprs,
            fieldNames);
    }

    /**
     * Creates a relational expression which projects a set of expressions.
     *
     * @param child input relational expression
     * @param exprs set of expressions for the input columns
     * @param fieldNames aliases of the expressions, or null to generate
     */
    public static ProjectRel createProject(
        RelNode child,
        RexNode [] exprs,
        String [] fieldNames)
    {
        return (ProjectRel) createProject(child, exprs, fieldNames, false);
    }

    public static RelNode createProject(
        RelNode child,
        List<Integer> posList)
    {
        RexNode [] exprList = new RexNode[posList.size()];

        final RelOptCluster cluster = child.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        for (int i = 0; i < posList.size(); i++) {
            exprList[i] =
                rexBuilder.makeInputRef(
                    (child.getRowType().getFields()[posList.get(i)]).getType(),
                    posList.get(i));
        }

        return CalcRel.createProject(
            child,
            exprList,
            null);
    }

    /**
     * Creates a relational expression which projects a set of expressions.
     *
     * <p>The result may not be a {@link ProjectRel}. If the projection is
     * trivial, <code>child</code> is returned directly; and future versions may
     * return other forumlations of expressions, such as {@link CalcRel}.
     *
     * @param child input relational expression
     * @param exprs set of expressions for the input columns
     * @param fieldNames aliases of the expressions, or null to generate
     * @param optimize Whether to return <code>child</code> unchanged if the
     * projections are trivial.
     */
    public static RelNode createProject(
        RelNode child,
        RexNode [] exprs,
        String [] fieldNames,
        boolean optimize)
    {
        assert (fieldNames == null) || (fieldNames.length == exprs.length);
        final RelOptCluster cluster = child.getCluster();
        RexProgramBuilder builder =
            new RexProgramBuilder(
                child.getRowType(),
                cluster.getRexBuilder());
        int i = -1;
        for (RexNode expr : exprs) {
            ++i;
            final String fieldName =
                (fieldNames == null) ? null : fieldNames[i];
            builder.addProject(expr, fieldName);
        }
        final RexProgram program = builder.getProgram();
        final List<RelCollation> collationList =
            program.getCollations(child.getCollationList());
        if (DeprecateProjectAndFilter) {
            return new CalcRel(
                cluster,
                RelOptUtil.clone(child.getTraits()),
                child,
                program.getOutputRowType(),
                program,
                collationList);
        } else {
            final RelDataType rowType =
                RexUtil.createStructType(
                    child.getCluster().getTypeFactory(),
                    exprs,
                    fieldNames);
            if (optimize
                && RemoveTrivialProjectRule.isIdentity(
                    exprs,
                    rowType,
                    child.getRowType()))
            {
                return child;
            }
            final ProjectRel project =
                new ProjectRel(
                    child.getCluster(),
                    child,
                    exprs,
                    rowType,
                    ProjectRelBase.Flags.Boxed,
                    collationList);

            return project;
        }
    }

    /**
     * Creates a relational expression which filters according to a given
     * condition, returning the same fields as its input.
     *
     * @param child Child relational expression
     * @param condition Condition
     *
     * @return Relational expression
     */
    public static RelNode createFilter(
        RelNode child,
        RexNode condition)
    {
        if (DeprecateProjectAndFilter) {
            final RelOptCluster cluster = child.getCluster();
            RexProgramBuilder builder =
                new RexProgramBuilder(
                    child.getRowType(),
                    cluster.getRexBuilder());
            builder.addIdentity();
            builder.addCondition(condition);
            final RexProgram program = builder.getProgram();
            return new CalcRel(
                cluster,
                RelOptUtil.clone(child.getTraits()),
                child,
                program.getOutputRowType(),
                program,
                RelCollation.emptyList);
        } else {
            return new FilterRel(
                child.getCluster(),
                child,
                condition);
        }
    }

    /**
     * Returns a relational expression which has the same fields as the
     * underlying expression, but the fields have different names.
     *
     * @param rel Relational expression
     * @param fieldNames Field names
     *
     * @return Renamed relational expression
     */
    public static RelNode createRename(
        RelNode rel,
        String [] fieldNames)
    {
        final RelDataTypeField [] fields = rel.getRowType().getFields();
        assert fieldNames.length == fields.length;
        final RexInputRef [] refs = new RexInputRef[fieldNames.length];
        for (int i = 0; i < refs.length; i++) {
            refs[i] =
                new RexInputRef(
                    i,
                    fields[i].getType());
        }
        return createProject(rel, refs, fieldNames, true);
    }

    public void collectVariablesUsed(Set<String> variableSet)
    {
        final RelOptUtil.VariableUsedVisitor vuv =
            new RelOptUtil.VariableUsedVisitor();
        for (RexNode expr : program.getExprList()) {
            expr.accept(vuv);
        }
        variableSet.addAll(vuv.variables);
    }

    /**
     * Creates a relational expression which permutes the output fields of a
     * relational expression according to a permutation.
     *
     * <p>Optimizations:
     *
     * <ul>
     * <li>If the relational expression is a {@link CalcRel} or {@link
     * ProjectRel} which is already acting as a permutation, combines the new
     * permuation with the old;</li>
     * <li>If the permutation is the identity, returns the original relational
     * expression.</li>
     * </ul>
     * </p>
     *
     * <p>If a permutation is combined with its inverse, these optimizations
     * would combine to remove them both.
     *
     * @param rel
     * @param permutation
     * @param fieldNames Field names; if null, or if a particular entry is null,
     * the name of the permuted field is used
     *
     * @return relational expression which permutes its input fields
     */
    public static RelNode permute(
        RelNode rel,
        Permutation permutation,
        List<String> fieldNames)
    {
        if (permutation.isIdentity()) {
            return rel;
        }
        if (rel instanceof CalcRel) {
            CalcRel calcRel = (CalcRel) rel;
            Permutation permutation1 = calcRel.getProgram().getPermutation();
            if (permutation1 != null) {
                Permutation permutation2 = permutation.product(permutation1);
                return permute(rel, permutation2, null);
            }
        }
        if (rel instanceof ProjectRel) {
            Permutation permutation1 = ((ProjectRel) rel).getPermutation();
            if (permutation1 != null) {
                Permutation permutation2 = permutation.product(permutation1);
                return permute(rel, permutation2, null);
            }
        }
        final List<RelDataType> outputTypeList = new ArrayList<RelDataType>();
        final List<String> outputNameList = new ArrayList<String>();
        final List<RexNode> exprList = new ArrayList<RexNode>();
        final List<RexLocalRef> projectRefList = new ArrayList<RexLocalRef>();
        final RelDataTypeField [] fields = rel.getRowType().getFields();
        for (int i = 0; i < permutation.getTargetCount(); i++) {
            int target = permutation.getTarget(i);
            final RelDataTypeField targetField = fields[target];
            outputTypeList.add(targetField.getType());
            outputNameList.add(
                ((fieldNames == null)
                    || (fieldNames.size() <= i)
                    || (fieldNames.get(i) == null)) ? targetField.getName()
                : fieldNames.get(i));
            exprList.add(
                rel.getCluster().getRexBuilder().makeInputRef(
                    fields[i].getType(),
                    i));
            final int source = permutation.getSource(i);
            projectRefList.add(
                new RexLocalRef(
                    source,
                    fields[source].getType()));
        }
        final RexProgram program =
            new RexProgram(
                rel.getRowType(),
                exprList,
                projectRefList,
                null,
                rel.getCluster().getTypeFactory().createStructType(
                    outputTypeList,
                    outputNameList));
        return new CalcRel(
            rel.getCluster(),
            rel.getTraits(),
            rel,
            program.getOutputRowType(),
            program,
            RelCollation.emptyList);
    }

    /**
     * Creates a relational expression which projects the output fields of a
     * relational expression according to a partial mapping.
     *
     * <p>A partial mapping is weaker than a permutation: every target has one
     * source, but a source may have 0, 1 or more than one targets. Usually the
     * result will have fewer fields than the source, unless some source fields
     * are projected multiple times.
     *
     * <p>This method could optimize the result as {@link #permute} does, but
     * does not at present.
     *
     * @param rel Relational expression
     * @param mapping Mapping from source fields to target fields. The mapping
     * type must obey the constaints {@link MappingType#isMandatorySource()} and
     * {@link MappingType#isSingleSource()}, as does {@link
     * MappingType#InverseFunction}.
     * @param fieldNames Field names; if null, or if a particular entry is null,
     * the name of the permuted field is used
     *
     * @return relational expression which projects a subset of the input fields
     */
    public static RelNode projectMapping(
        RelNode rel,
        Mapping mapping,
        List<String> fieldNames)
    {
        assert mapping.getMappingType().isSingleSource();
        assert mapping.getMappingType().isMandatorySource();
        if (mapping.isIdentity()) {
            return rel;
        }
        final List<RelDataType> outputTypeList = new ArrayList<RelDataType>();
        final List<String> outputNameList = new ArrayList<String>();
        final List<RexNode> exprList = new ArrayList<RexNode>();
        final List<RexLocalRef> projectRefList = new ArrayList<RexLocalRef>();
        final RelDataTypeField [] fields = rel.getRowType().getFields();
        for (int i = 0; i < fields.length; i++) {
            final RelDataTypeField field = fields[i];
            exprList.add(
                rel.getCluster().getRexBuilder().makeInputRef(
                    field.getType(),
                    i));
        }
        for (int i = 0; i < mapping.getTargetCount(); i++) {
            int source = mapping.getSource(i);
            final RelDataTypeField sourceField = fields[source];
            outputTypeList.add(sourceField.getType());
            outputNameList.add(
                ((fieldNames == null)
                    || (fieldNames.size() <= i)
                    || (fieldNames.get(i) == null)) ? sourceField.getName()
                : fieldNames.get(i));
            projectRefList.add(new RexLocalRef(
                    source,
                    sourceField.getType()));
        }
        final RexProgram program =
            new RexProgram(
                rel.getRowType(),
                exprList,
                projectRefList,
                null,
                rel.getCluster().getTypeFactory().createStructType(
                    outputTypeList,
                    outputNameList));
        return new CalcRel(
            rel.getCluster(),
            rel.getTraits(),
            rel,
            program.getOutputRowType(),
            program,
            RelCollation.emptyList);
    }
}

// End CalcRel.java
