/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.rel;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;


/**
 * A JoinRel represents two relational expressions joined according to some
 * condition.
 */
public class JoinRel extends SaffronRel
{
    //~ Instance fields -------------------------------------------------------

    protected RexNode condition;
    protected SaffronRel left;
    protected SaffronRel right;
    protected Set variablesStopped = Collections.EMPTY_SET;

    /**
     * Values must be of enumeration {@link JoinType}, except that {@link
     * JoinType#RIGHT} is disallowed.
     */
    protected int joinType;

    //~ Constructors ----------------------------------------------------------

    public JoinRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right,
        RexNode condition,
        int joinType,
        Set variablesStopped)
    {
        super(cluster);
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.variablesStopped = variablesStopped;
        assert(
            (joinType == JoinType.INNER) || (joinType == JoinType.LEFT)
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

    public SaffronRel [] getInputs()
    {
        return new SaffronRel [] { left,right };
    }

    public int getJoinType()
    {
        return joinType;
    }

    public SaffronRel getLeft()
    {
        return left;
    }

    public SaffronRel getRight()
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
        visitor.visit(left,0,this);
        visitor.visit(right,1,this);
    }

    public Object clone()
    {
        return new JoinRel(
            cluster,
            OptUtil.clone(left),
            OptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            new HashSet(variablesStopped));
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left","right","condition","joinType" },
            new Object [] { JoinType.toString(joinType) });
    }

    public void registerStoppedVariable(String name)
    {
        if (variablesStopped.isEmpty()) {
            variablesStopped = new HashSet();
        }
        variablesStopped.add(name);
    }

    public void replaceInput(int ordinalInParent,SaffronRel p)
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

    protected SaffronType deriveRowType()
    {
        SaffronType leftType = left.getRowType();
        SaffronType rightType = right.getRowType();

        switch(joinType) {
        case JoinType.LEFT:
            rightType = cluster.typeFactory.createTypeWithNullability(
                rightType,true);
            break;
        case JoinType.RIGHT:
            leftType = cluster.typeFactory.createTypeWithNullability(
                leftType,true);
            break;
        case JoinType.FULL:
            leftType = cluster.typeFactory.createTypeWithNullability(
                leftType,true);
            rightType = cluster.typeFactory.createTypeWithNullability(
                rightType,true);
            break;
        default:
            break;
        }
        return createJoinType(cluster.typeFactory, leftType, rightType);
    }

    public static SaffronType createJoinType(
        SaffronTypeFactory typeFactory,
        SaffronType leftType, SaffronType rightType)
    {
        ArrayList nameList = new ArrayList();
        ArrayList typeList = new ArrayList();
        addFields(leftType, typeList, nameList);
        addFields(rightType, typeList, nameList);
        String [] fieldNames = (String [])
                nameList.toArray(new String[nameList.size()]);
        SaffronType [] types = (SaffronType [])
                typeList.toArray(new SaffronType[typeList.size()]);
        return typeFactory.createProjectType(types, fieldNames);
    }

    private static void addFields(SaffronType type, ArrayList typeList,
            ArrayList nameList) {
        final SaffronField [] fields = type.getFields();
        for (int i = 0; i < fields.length; i++) {
            SaffronField field = fields[i];
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
