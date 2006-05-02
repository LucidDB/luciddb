/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
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
import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

/**
 * <code>JoinRelBase</code> is an abstract base class for
 * implementations of {@link JoinRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JoinRelBase extends AbstractRelNode
{
    //~ Instance fields -------------------------------------------------------

    protected RexNode condition;
    protected RelNode left;
    protected RelNode right;
    protected Set<String> variablesStopped = Collections.emptySet();

    /**
     * Values must be of enumeration {@link JoinRelType}, except that {@link
     * JoinType#RIGHT} is disallowed.
     */
    protected JoinRelType joinType;

    protected JoinRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
    {
        super(cluster, traits);
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.variablesStopped = variablesStopped;
        assert joinType != null;
        assert condition != null;
        this.joinType = joinType;
    }

    public RexNode [] getChildExps()
    {
        return new RexNode [] { condition };
    }

    public RexNode getCondition()
    {
        return condition;
    }

    public RelNode [] getInputs()
    {
        return new RelNode [] { left, right };
    }

    public JoinRelType getJoinType()
    {
        return joinType;
    }

    public RelNode getLeft()
    {
        return left;
    }

    public RelNode getRight()
    {
        return right;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // REVIEW jvs 9-Apr-2006:  Just for now...
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(rowCount, 0, 0);
    }

    public static double estimateJoinedRows(
        JoinRelBase joinRel, RexNode condition)
    {
        double product = RelMetadataQuery.getRowCount(joinRel.getLeft())
            * RelMetadataQuery.getRowCount(joinRel.getRight());
        // TODO:  correlation factor
        return product * RelMetadataQuery.getSelectivity(joinRel, condition);
    }

    // implement RelNode
    public double getRows()
    {
        return estimateJoinedRows(this, condition);
    }

    public void setVariablesStopped(Set<String> set)
    {
        variablesStopped = set;
    }

    public Set<String> getVariablesStopped()
    {
        return variablesStopped;
    }

    public void childrenAccept(RelVisitor visitor)
    {
        visitor.visit(left, 0, this);
        visitor.visit(right, 1, this);
    }
    
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left", "right", "condition", "joinType" },
            new Object [] { joinType.name().toLowerCase() });
    }

    public void registerStoppedVariable(String name)
    {
        if (variablesStopped.isEmpty()) {
            variablesStopped = new HashSet<String>();
        }
        variablesStopped.add(name);
    }

    public void replaceInput(
        int ordinalInParent,
        RelNode p)
    {
        switch (ordinalInParent) {
        case 0:
            this.left = p;
            break;
        case 1:
            this.right = p;
            break;
        default:
            throw Util.newInternal();
        }
    }

    protected RelDataType deriveRowType()
    {
        return deriveJoinRowType(
            left.getRowType(), right.getRowType(), joinType,
            getCluster().getTypeFactory(), null);
    }

    public static RelDataType deriveJoinRowType(
        RelDataType leftType,
        RelDataType rightType,
        JoinRelType joinType,
        RelDataTypeFactory typeFactory,
        List<String> fieldNameList)
    {
        switch (joinType) {
        case LEFT:
            rightType =
                typeFactory.createTypeWithNullability(rightType, true);
            break;
        case RIGHT:
            leftType =
                typeFactory.createTypeWithNullability(leftType, true);
            break;
        case FULL:
            leftType =
                typeFactory.createTypeWithNullability(leftType, true);
            rightType =
                typeFactory.createTypeWithNullability(rightType, true);
            break;
        default:
            break;
        }
        return createJoinType(typeFactory, leftType, rightType, fieldNameList);
    }

    /**
     * Returns the type of joining two relations. The result type consists of
     * the fields of the left type plus the fields of the right type. The
     * field name list, if present, overrides the original names of the fields.
     *
     * @param typeFactory Type factory
     * @param leftType Type of left input to join
     * @param rightType Type of right input to join
     * @param fieldNameList If not null, overrides the original names of the
     *                 fields
     * @return
     * @pre fieldNameList == null ||
     *   fieldNameList.size() ==
     *   leftType.getFields().length +
     *   rightType.getFields().length
     */
    public static RelDataType createJoinType(
        RelDataTypeFactory typeFactory,
        RelDataType leftType,
        RelDataType rightType,
        List<String> fieldNameList)
    {
        assert fieldNameList == null ||
            fieldNameList.size() ==
            leftType.getFields().length +
            rightType.getFields().length;
        List<String> nameList = new ArrayList<String>();
        List<RelDataType> typeList = new ArrayList<RelDataType>();
        addFields(leftType, typeList, nameList);
        if (rightType != null) {
            addFields(rightType, typeList, nameList);
        }
        if (fieldNameList != null) {
            assert fieldNameList.size() == nameList.size();
            nameList = fieldNameList;
        }
        return typeFactory.createStructType(typeList, nameList);
    }

    private static void addFields(
        RelDataType type,
        List<RelDataType> typeList,
        List<String> nameList)
    {
        final RelDataTypeField [] fields = type.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            String name = field.getName();

            // Ensure that name is unique from all previous field names
            if (nameList.contains(name)) {
                String nameBase = name;
                for (int j = 0;; j++) {
                    name = nameBase + j;
                    if (!nameList.contains(name)) {
                        break;
                    }
                }
            }
            nameList.add(name);
            typeList.add(field.getType());
        }
    }

    //~ Inner Classes ---------------------------------------------------------

}

// End JoinRelBase.java
