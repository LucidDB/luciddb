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

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;

/**
 * <code>ProjectRelBase</code> is an abstract base class for implementations
 * of {@link ProjectRel}.
 */
public abstract class ProjectRelBase extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected RexNode [] exps;

    /** Values defined in {@link Flags}. */
    protected int flags;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Project.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param traits traits of this rel
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link Flags}
     */
    protected ProjectRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags)
    {
        super(cluster, traits, child);
        assert rowType != null;
        this.exps = exps;
        this.rowType = rowType;
        this.flags = flags;
        assert isValid(true);
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isBoxed()
    {
        return (flags & Flags.Boxed) == Flags.Boxed;
    }

    // override AbstractRelNode
    public RexNode[] getChildExps()
    {
        return getProjectExps();
    }

    /**
     * Returns the project expressions.
     */
    public RexNode [] getProjectExps()
    {
        return exps;
    }

    public int getFlags()
    {
        return flags;
    }

    public boolean isValid(boolean fail)
    {
        if (!super.isValid(fail)) {
            return false;
        }
        if (!RexUtil.compatibleTypes(exps, getRowType(), true)) {
            return false;
        }
        Checker checker = new Checker(fail, getChild());
        for (RexNode exp : exps) {
            exp.accept(checker);
        }
        if (checker.failCount > 0) {
            return false;
        }
        if (!isBoxed()) {
            if (exps.length != 1) {
                return false;
            }
        }
        return true;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getChild().getRows();
        double dCpu = getChild().getRows() * exps.length;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    protected void defineTerms(String [] terms)
    {
        int i = 0;
        terms[i++] = "child";
        final RelDataTypeField[] fields = rowType.getFields();
        for (int j = 0; j < fields.length; j++) {
            String fieldName = fields[j].getName();
            if (fieldName == null) {
                fieldName = "field#" + j;
            }
            terms[i++] = fieldName;
        }
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[1 + exps.length];
        defineTerms(terms);
        pw.explain(this, terms);
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation
     * which provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRelImplementor implementor,
        String fieldName)
    {
        if (!isBoxed()) {
            return implementor.implementFieldAccess(
                (JavaRel) getChild(), fieldName);
        }
        RelDataType type = getRowType();
        int field = type.getFieldOrdinal(fieldName);
        return implementor.findRel((JavaRel) this, exps[field]);
    }

    //~ Inner Interfaces ------------------------------------------------------

    public interface Flags
    {
        int AnonFields = 2;

        /**
         * Whether the resulting row is to be a synthetic class whose fields
         * are the aliases of the fields. <code>boxed</code> must be true
         * unless there is only one field: <code>select {dept.deptno} from
         * dept</code> is boxed, <code>select dept.deptno from dept</code> is
         * not.
         */
        int Boxed = 1;
        int None = 0;
    }

    /**
     * Visitor which walks over a program and checks validity.
     */
    private static class Checker extends RexVisitorImpl
    {
        private final boolean fail;
        private final RelNode child;
        int failCount = 0;

        public Checker(boolean fail, RelNode child)
        {
            super(true);
            this.fail = fail;
            this.child = child;
        }

        public void visitInputRef(RexInputRef inputRef)
        {
            final int index = inputRef.getIndex();
            final RelDataTypeField[] fields = child.getRowType().getFields();
            if (index < 0 || index >= fields.length) {
                assert !fail;
                ++failCount;
            }
            if (!RelOptUtil.eq(
                "inputRef", inputRef.getType(),
                "underlying field", fields[index].getType(), fail)) {
                assert !fail;
                ++failCount;
            }
        }

        public void visitLocalRef(RexLocalRef localRef)
        {
            assert !fail : "localRef invalid in project";
            ++failCount;
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            super.visitFieldAccess(fieldAccess);
            final RelDataType refType =
                fieldAccess.getReferenceExpr().getType();
            assert refType.isStruct();
            final RelDataTypeField field = fieldAccess.getField();
            final int index = field.getIndex();
            if (index < 0 || index > refType.getFields().length) {
                assert !fail;
                ++failCount;
            }
            final RelDataTypeField typeField = refType.getFields()[index];
            if (!RelOptUtil.eq("type1", typeField.getType(), "type2", fieldAccess.getType(), fail)) {
                assert !fail;
                ++failCount;
            }
        }
    }
}


// End ProjectRelBase.java
