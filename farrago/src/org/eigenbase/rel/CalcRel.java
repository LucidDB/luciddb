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

package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;

import java.util.Set;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link ProjectRel} and {@link FilterRel}. It should be created in the
 * latter stages of optimization, by merging consecutive {@link ProjectRel}
 * and {@link FilterRel} nodes together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:<ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a
 *     {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a
 *     {@link ProjectRel}</li>
 * <li>{@link MergeCalcRule} merges two CalcRels</li>
 * </ul></p>
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public final class CalcRel extends CalcRelBase
{
    public static final boolean DeprecateProjectAndFilter = false;
    //~ Constructors ----------------------------------------------------------

    public CalcRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        RexProgram program)
    {
        super(cluster, traits, child, rowType, program);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new CalcRel(
            getCluster(), cloneTraits(), getChild(), rowType,
            program.copy());
    }

    /**
     * Creates a relational expression which projects a set of expressions.
     *
     * @param child input relational expression
     * @param exprs set of expressions for the input columns
     * @param fieldNames aliases of the expressions, or null to generate
     */
    public static RelNode createProject(
        RelNode child,
        RexNode[] exprs,
        String[] fieldNames)
    {
        if (DeprecateProjectAndFilter) {
            assert fieldNames == null || fieldNames.length == exprs.length;
            final RelOptCluster cluster = child.getCluster();
            RexProgramBuilder builder = new RexProgramBuilder(
                child.getRowType(), cluster.getRexBuilder());
            int i = -1;
            for (RexNode expr : exprs) {
                ++i;
                final String fieldName = fieldNames == null ? null : fieldNames[i];
                builder.addProject(expr, fieldName);
            }
            final RexProgram program = builder.getProgram();
            return new CalcRel(
                cluster,
                RelOptUtil.clone(child.getTraits()),
                child,
                program.getOutputRowType(),
                program);
        } else {
            final ProjectRel project =
                new ProjectRel(
                    child.getCluster(), child, exprs, fieldNames,
                    ProjectRelBase.Flags.Boxed);

            return project;
        }
    }

    /**
     * Creates a relational expression which filters according to a given
     * condition, returning the same fields as its input.
     *
     * @param child Child relational expression
     * @param condition Condition
     * @return Relational expression
     */
    public static RelNode createFilter(
        RelNode child,
        RexNode condition)
    {
        if (DeprecateProjectAndFilter) {
            final RelOptCluster cluster = child.getCluster();
            RexProgramBuilder builder =
                new RexProgramBuilder(child.getRowType(), cluster.getRexBuilder());
            builder.addIdentity();
            builder.addCondition(condition);
            final RexProgram program = builder.getProgram();
            return new CalcRel(
                cluster,
                RelOptUtil.clone(child.getTraits()),
                child,
                program.getOutputRowType(),
                program);
        } else {
            return new FilterRel(child.getCluster(), child, condition);
        }
    }

    /**
     * Returns a relational expression which has the same fields as the
     * underlying expression, but the fields have different names.
     *
     * @param rel Relational expression
     * @param fieldNames Field names
     * @return Renamed relational expression
     */
    public static RelNode createRename(
        RelNode rel,
        String[] fieldNames)
    {
        final RelDataTypeField[] fields = rel.getRowType().getFields();
        assert fieldNames.length == fields.length;
        final RexInputRef[] refs = new RexInputRef[fieldNames.length];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = new RexInputRef(i, fields[i].getType());
        }
        return createProject(rel, refs, fieldNames);
    }

    public void collectVariablesUsed(Set variableSet)
    {
        final RelOptUtil.VariableUsedVisitor vuv =
            new RelOptUtil.VariableUsedVisitor();
        for (RexNode expr : program.getExprList()) {
            expr.accept(vuv);
        }
        variableSet.addAll(vuv.variables);
    }
}

// End CalcRel.java
