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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.Util;


/**
 * A JoinRel represents two relational expressions joined according to some
 * condition.
 */
public class JoinRel extends AbstractRelNode
{
    //~ Instance fields -------------------------------------------------------

    protected RexNode condition;
    protected RelNode left;
    protected RelNode right;
    protected Set variablesStopped = Collections.EMPTY_SET;

    /**
     * Values must be of enumeration {@link JoinType}, except that {@link
     * JoinType#RIGHT} is disallowed.
     */
    protected int joinType;

    //~ Constructors ----------------------------------------------------------

    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        int joinType,
        Set variablesStopped)
    {
        this(
            cluster, new RelTraitSet(CallingConvention.NONE), left, right,
            condition, joinType, variablesStopped);
    }

    protected JoinRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        RexNode condition,
        int joinType,
        Set variablesStopped)
    {
        super(cluster, traits);
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.variablesStopped = variablesStopped;
        assert ((joinType == JoinType.INNER) || (joinType == JoinType.LEFT)
            || (joinType == JoinType.FULL)); // RIGHT not allowed
        this.joinType = joinType;
    }

    //~ Methods ---------------------------------------------------------------

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

    public int getJoinType()
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

    public void setVariablesStopped(HashSet set)
    {
        variablesStopped = set;
    }

    public Set getVariablesStopped()
    {
        return variablesStopped;
    }

    public void childrenAccept(RelVisitor visitor)
    {
        visitor.visit(left, 0, this);
        visitor.visit(right, 1, this);
    }

    public Object clone()
    {
        return new JoinRel(
            cluster,
            cloneTraits(),
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            new HashSet(variablesStopped));
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left", "right", "condition", "joinType" },
            new Object [] { JoinType.toString(joinType) });
    }

    public void registerStoppedVariable(String name)
    {
        if (variablesStopped.isEmpty()) {
            variablesStopped = new HashSet();
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

    public int switchJoinType(int joinType)
    {
        switch (joinType) {
        case JoinRel.JoinType.LEFT:
            return JoinRel.JoinType.RIGHT;
        case JoinRel.JoinType.RIGHT:
            return JoinRel.JoinType.LEFT;
        case JoinRel.JoinType.INNER:
        case JoinRel.JoinType.FULL:
            return joinType;
        default:
            throw Util.newInternal("invalid join type " + joinType);
        }
    }

    protected RelDataType deriveRowType()
    {
        return deriveJoinRowType(left, right, joinType, cluster.typeFactory);

    }

    public static RelDataType deriveJoinRowType(RelNode left,
        RelNode right,
        int joinType,
        RelDataTypeFactory typeFactory)
    {
        RelDataType leftType = left.getRowType();
        RelDataType rightType = right.getRowType();

        switch (joinType) {
        case JoinType.LEFT:
            rightType =
                typeFactory.createTypeWithNullability(rightType, true);
            break;
        case JoinType.RIGHT:
            leftType =
                typeFactory.createTypeWithNullability(leftType, true);
            break;
        case JoinType.FULL:
            leftType =
                typeFactory.createTypeWithNullability(leftType, true);
            rightType =
                typeFactory.createTypeWithNullability(rightType, true);
            break;
        default:
            break;
        }
        return createJoinType(typeFactory, leftType, rightType);
    }

    public static RelDataType createJoinType(
        RelDataTypeFactory typeFactory,
        RelDataType leftType,
        RelDataType rightType)
    {
        ArrayList nameList = new ArrayList();
        ArrayList typeList = new ArrayList();
        addFields(leftType, typeList, nameList);
        addFields(rightType, typeList, nameList);
        String [] fieldNames =
            (String []) nameList.toArray(new String[nameList.size()]);
        RelDataType [] types =
            (RelDataType []) typeList.toArray(
                new RelDataType[typeList.size()]);
        return typeFactory.createStructType(types, fieldNames);
    }

    private static void addFields(
        RelDataType type,
        ArrayList typeList,
        ArrayList nameList)
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

    /**
     * Enumeration of join types.
     */
    public static abstract class JoinType
    {
        public static final int INNER = 0;
        public static final int LEFT = 1;
        public static final int RIGHT = 2;
        public static final int FULL = 3;

        public static final String toString(int i)
        {
            switch (i) {
            case INNER:
                return "inner";
            case LEFT:
                return "left";
            case RIGHT:
                return "right";
            case FULL:
                return "full";
            default:
                throw Util.newInternal("bad JoinType " + i);
            }
        }
    }
}


// End JoinRel.java
