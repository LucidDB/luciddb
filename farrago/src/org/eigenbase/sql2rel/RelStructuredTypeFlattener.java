/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
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
package org.eigenbase.sql2rel;

import org.eigenbase.sql.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.type.*;

import java.util.*;

// TODO jvs 10-Feb-2005:  factor out generic rewrite helper, with the
// ability to map between old and new rels and field ordinals.  Also,
// for now need to prohibit queries which return UDT instances.

/**
 * RelStructuredTypeFlattener removes all structured types from a tree of
 * relational expressions.  Because it must operate globally on the tree, it is
 * implemented as an explicit self-contained rewrite operation instead of via
 * normal optimizer rules.  This approach has the benefit that real optimizer
 * and codegen rules never have to deal with structured types.
 *
 *<p>
 *
 * As an example, suppose we have a structured type <code>ST(A1 smallint, A2
 * bigint)</code>, a table <code>T(c1 ST, c2 double)</code>, and a query
 * <code>select t.c2, t.c1.a2 from t</code>.  After SqlToRelConverter
 * executes, the unflattened tree looks like:
 *
 *<pre><code>
 * ProjectRel(C2=[$1], A2=[$0.A2])
 *   TableAccessRel(table=[T])
 *</code></pre>
 *
 * After flattening, the resulting tree looks like
 *
 *<pre><code>
 * ProjectRel(C2=[$2], A2=[$1])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 *</code></pre>
 *
 * The index scan produces a flattened row type <code>(smallint, bigint,
 * double)</code>, and the projection picks out the desired attributes
 * (omitting <code>$0</code> altogether).  After optimization, the projection
 * might be pushed down into the index scan, resulting in a final tree like
 *
 *<pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[2, 1])
 *</code></pre>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelStructuredTypeFlattener
{
    private final RexBuilder rexBuilder;
    private final RewriteRelVisitor visitor;
    
    private Map oldToNewRelMap;
    private RelNode currentRel;

    public RelStructuredTypeFlattener(RexBuilder rexBuilder)
    {
        this.rexBuilder = rexBuilder;
        visitor = new RewriteRelVisitor();
    }
    
    public RelNode rewrite(RelNode root)
    {
        oldToNewRelMap = new HashMap();
        visitor.visit(root, 0, null);
        return getNewForOldRel(root);
    }

    private void setNewForOldRel(RelNode oldRel, RelNode newRel)
    {
        oldToNewRelMap.put(oldRel, newRel);
    }

    private RelNode getNewForOldRel(RelNode oldRel)
    {
        return (RelNode) oldToNewRelMap.get(oldRel);
    }

    private int getNewForOldInput(int oldOrdinal)
    {
        assert(currentRel != null);
        int newOrdinal = 0;

        // determine which input rel oldOrdinal references, and adjust
        // oldOrdinal to be relative to that input rel
        RelNode [] oldInputs = currentRel.getInputs();
        RelNode oldInput = null;
        for (int i = 0; i < oldInputs.length; ++i) {
            RelDataType oldInputType = oldInputs[i].getRowType();
            int n = oldInputType.getFieldList().size();
            if (oldOrdinal < n) {
                oldInput = oldInputs[i];
                break;
            }
            RelNode newInput = getNewForOldRel(oldInputs[i]);
            newOrdinal += newInput.getRowType().getFieldList().size();
            oldOrdinal -= n;
        }
        assert (oldInput != null);

        RelDataType oldInputType = oldInput.getRowType();
        newOrdinal += calculateFlattenedOffset(oldInputType, oldOrdinal);
        return newOrdinal;
    }

    private int calculateFlattenedOffset(
        RelDataType rowType,
        int ordinal)
    {
        int offset = 0;
        RelDataTypeField [] oldFields = rowType.getFields();
        for (int i = 0; i < ordinal; ++i) {
            RelDataType oldFieldType = oldFields[i].getType();
            if (oldFieldType.isStruct()) {
                // TODO jvs 10-Feb-2005:  this isn't terribly efficient;
                // keep a mapping somewhere
                RelDataType flattened = SqlTypeUtil.flattenRecordType(
                    rexBuilder.getTypeFactory(),
                    oldFieldType,
                    null);
                offset += flattened.getFieldList().size();
            } else {
                ++offset;
            }
        }
        return offset;
    }

    private RexNode flattenFieldAccesses(RexNode exp)
    {
        RewriteRexShuttle shuttle = new RewriteRexShuttle();
        return shuttle.visit(exp);
    }

    public void rewriteRel(TableModificationRel rel)
    {
        TableModificationRel newRel = new TableModificationRel(
            rel.getCluster(),
            rel.getTable(),
            rel.getConnection(),
            getNewForOldRel(rel.child),
            rel.getOperation(),
            rel.getUpdateColumnList(),
            true);
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(AggregateRel rel)
    {
        RelDataType inputType = rel.child.getRowType();
        Iterator fields = inputType.getFieldList().iterator();
        while (fields.hasNext()) {
            RelDataTypeField field = (RelDataTypeField) fields.next();
            if (field.getType().isStruct()) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement("aggregation on structured types");
            }
        }
            
        rewriteGeneric(rel);
    }

    public void rewriteRel(SortRel rel)
    {
        RelFieldCollation [] oldCollations = rel.getCollations();
        RelFieldCollation [] newCollations =
            new RelFieldCollation[oldCollations.length];
        for (int i = 0; i < oldCollations.length; ++i) {
            int oldInput = oldCollations[i].iField;
            RelDataType sortFieldType = 
                rel.child.getRowType().getFields()[oldInput].getType();
            if (sortFieldType.isStruct()) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement("sorting on structured types");
            }
            newCollations[i] = new RelFieldCollation(
                getNewForOldInput(oldInput));
        }
        SortRel newRel = new SortRel(
            rel.getCluster(),
            getNewForOldRel(rel.child),
            newCollations);
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(FilterRel rel)
    {
        FilterRel newRel = new FilterRel(
            rel.getCluster(),
            getNewForOldRel(rel.child),
            flattenFieldAccesses(rel.condition));
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(JoinRel rel)
    {
        JoinRel newRel = new JoinRel(
            rel.getCluster(),
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            flattenFieldAccesses(rel.getCondition()),
            rel.getJoinType(),
            rel.getVariablesStopped());
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(CorrelatorRel rel)
    {
        Iterator oldCorrelations = rel.getCorrelations().iterator();
        ArrayList newCorrelations = new ArrayList();
        while (oldCorrelations.hasNext()) {
            CorrelatorRel.Correlation c = (CorrelatorRel.Correlation)
                oldCorrelations.next();
            RelDataType corrFieldType =
                rel.getLeft().getRowType().getFields()[c.offset].getType();
            if (corrFieldType.isStruct()) {
                throw Util.needToImplement("correlation on structured type");
            }
            newCorrelations.add(
                new CorrelatorRel.Correlation(
                    c.id,
                    getNewForOldInput(c.offset)));
        }
        CorrelatorRel newRel = new CorrelatorRel(
            rel.getCluster(), 
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            newCorrelations);
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(DistinctRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(CollectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(UncollectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(IntersectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(MinusRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(UnionRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(OneRowRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(ProjectRel rel)
    {
        List flattenedExps = new ArrayList();
        List flattenedFieldNames = new ArrayList();
        flattenProjections(
            rel.getChildExps(),
            rel.getFieldNames(),
            flattenedExps,
            flattenedFieldNames);
        RexNode [] newExps =
            (RexNode []) flattenedExps.toArray(RexNode.EMPTY_ARRAY);
        String [] newFieldNames =
            (String []) flattenedFieldNames.toArray(Util.emptyStringArray);
        ProjectRel newRel = new ProjectRel(
            rel.getCluster(),
            getNewForOldRel(rel.child),
            newExps,
            newFieldNames,
            rel.getFlags());
        setNewForOldRel(rel, newRel);
    }

    private void rewriteGeneric(RelNode rel)
    {
        RelNode newRel = RelOptUtil.clone(rel);
        RelNode [] oldInputs = rel.getInputs();
        for (int i = 0; i < oldInputs.length; ++i) {
            newRel.replaceInput(
                i,
                getNewForOldRel(oldInputs[i]));
        }
        setNewForOldRel(rel, newRel);
    }

    private void flattenProjections(
        RexNode [] exps,
        String [] fieldNames,
        List flattenedExps,
        List flattenedFieldNames)
    {
        for (int i = 0; i < exps.length; ++i) {
            RexNode exp = exps[i];
            if (exp.getType().isStruct()) {
                if (exp instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) exp;
                    // expand to range
                    RelDataType flattenedType = SqlTypeUtil.flattenRecordType(
                        rexBuilder.getTypeFactory(),
                        exp.getType(),
                        null);
                    List fieldList = flattenedType.getFieldList();
                    int n = fieldList.size();
                    for (int j = 0; j < n; ++j) {
                        RelDataTypeField field = (RelDataTypeField)
                            fieldList.get(j);
                        flattenedExps.add(
                            new RexInputRef(
                                inputRef.index + j,
                                field.getType()));
                        flattenedFieldNames.add(null);
                    }
                } else if (isConstructor(exp)) {
                    RexCall call = (RexCall) exp;
                    flattenProjections(
                        call.getOperands(),
                        new String[call.getOperands().length],
                        flattenedExps,
                        flattenedFieldNames);
                } else if (exp instanceof RexCall) {
                    // NOTE jvs 10-Feb-2005:  This is a lame hack to
                    // keep special functions which return row types
                    // working.
                    Iterator fieldIter =
                        exp.getType().getFieldList().iterator();
                    while (fieldIter.hasNext()) {
                        RelDataTypeField field = (RelDataTypeField)
                            fieldIter.next();
                        RexNode cloneCall = RexUtil.clone(exp);
                        RexNode fieldAccess = rexBuilder.makeFieldAccess(
                            cloneCall, field.getIndex());
                        flattenedExps.add(fieldAccess);
                        flattenedFieldNames.add(null);
                    }
                } else {
                    // TODO UDT constructor
                    throw Util.needToImplement(exp);
                }
            } else {
                exp = flattenFieldAccesses(exp);
                flattenedExps.add(exp);
                if (fieldNames != null) {
                    flattenedFieldNames.add(fieldNames[i]);
                }
            }
        }
    }

    private boolean isConstructor(RexNode rexNode)
    {
        // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) rexNode;
        return call.getOperator().name.equalsIgnoreCase("row")
            || (call.isA(RexKind.NewSpecification));
    }

    public void rewriteRel(TableAccessRel rel)
    {
        // TODO jvs 10-Feb-2005:  reintroduce AbstractTableAccessRel
        // and get rid of this
        if (!rel.getClass().equals(TableAccessRel.class)) {
            setNewForOldRel(rel, rel);
            return;
        }
        
        RelNode newRel = rel.getTable().toRel(
            rel.getCluster(),
            rel.getConnection());
        
        setNewForOldRel(rel, newRel);
    }

    private class RewriteRelVisitor extends RelVisitor
    {
        // implement RelVisitor
        public void visit(RelNode p, int ordinal, RelNode parent)
        {
            // rewrite children first
            super.visit(p, ordinal, parent);

            currentRel = p;
            boolean found = ReflectUtil.invokeVisitor(
                RelStructuredTypeFlattener.this,
                currentRel,
                RelNode.class,
                "rewriteRel");
            currentRel = null;
            if (!found) {
                if (p.getInputs().length == 0) {
                    // for leaves, it's usually safe to assume that
                    // no transformation is required
                    rewriteGeneric(p);
                }
            }
            Util.permAssert(found, p.getClass().getName());
        }
    }

    private class RewriteRexShuttle extends RexShuttle
    {
        // override RexShuttle
        public RexNode visit(RexInputRef input)
        {
            RexInputRef newInput = new RexInputRef(
                getNewForOldInput(input.index),
                removeDistinct(input.getType()));
            return newInput;
        }

        private RelDataType removeDistinct(RelDataType type)
        {
            if (type.getSqlTypeName() != SqlTypeName.Distinct) {
                return type;
            }
            return type.getFields()[0].getType();
        }

        // override RexShuttle
        public RexNode visit(RexFieldAccess fieldAccess)
        {
            // walk down the field access path expression, calculating
            // the desired input number
            int iInput = 0;
            RelDataType fieldType = removeDistinct(fieldAccess.getType());
            
            for (;;) {
                RexNode refExp = fieldAccess.getReferenceExpr();
                int ordinal = refExp.getType().getFieldOrdinal(
                    fieldAccess.getField().getName());
                iInput += calculateFlattenedOffset(
                    refExp.getType(), ordinal);
                if (refExp instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) refExp;
                    iInput += getNewForOldInput(inputRef.index);
                    return new RexInputRef(iInput, fieldType);
                } else {
                    if (!(refExp instanceof RexFieldAccess)) {
                        throw Util.needToImplement(refExp);
                    }
                }
                fieldAccess = (RexFieldAccess) refExp;
            }

        }

        // override RexShuttle
        public RexNode visit(RexCall rexCall)
        {
            if (rexCall.isA(RexKind.Cast)) {
                RexNode input = visit(rexCall.getOperands()[0]);
                RelDataType targetType = removeDistinct(rexCall.getType());
                return rexBuilder.makeCast(
                    targetType,
                    input);
            }
            if (!rexCall.isA(RexKind.Comparison)) {
                return super.visit(rexCall);
            }
            RexNode lhs = rexCall.getOperands()[0];
            if (!lhs.getType().isStruct()) {
                return super.visit(rexCall);
            }
            List flattenedExps = new ArrayList();
            flattenProjections(
                rexCall.getOperands(),
                null,
                flattenedExps,
                new ArrayList());
            int n = flattenedExps.size() / 2;
            if ((n > 1) && !rexCall.isA(RexKind.Equals)) {
                throw Util.needToImplement(
                    "inequality comparison for row types");
            }
            RexNode conjunction = null;
            for (int i = 0; i < n; ++i) {
                RexNode comparison = rexBuilder.makeCall(
                    rexCall.getOperator(),
                    (RexNode) flattenedExps.get(i), 
                    (RexNode) flattenedExps.get(i + n));
                if (conjunction == null) {
                    conjunction = comparison;
                } else {
                    conjunction = rexBuilder.makeCall(
                        RexKind.And,
                        conjunction,
                        comparison);
                }
            }
            return conjunction;
        }
    }
}

// End RelStructuredTypeFlattener.java
