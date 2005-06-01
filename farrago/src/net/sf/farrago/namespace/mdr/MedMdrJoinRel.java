/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.namespace.mdr;

import java.util.*;
import java.util.List;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;
import org.netbeans.api.mdr.*;


/**
 * MedMdrJoinRel is the relational expression corresponding to a join via
 * association to an MedMdrClassExtent on the right hand side.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRel extends JoinRelBase implements JavaRel
{
    //~ Instance fields -------------------------------------------------------

    private int leftOrdinal;
    private Reference rightReference;

    //~ Constructors ----------------------------------------------------------

    MedMdrJoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        int joinType,
        int leftOrdinal,
        Reference rightReference)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.ITERATOR), left, right,
            condition, joinType, Collections.EMPTY_SET);
        assert ((joinType == JoinType.INNER) || (joinType == JoinType.LEFT));

        this.leftOrdinal = leftOrdinal;
        this.rightReference = rightReference;
    }

    //~ Methods ---------------------------------------------------------------

    int getLeftOrdinal()
    {
        return leftOrdinal;
    }

    Reference getRightReference()
    {
        return rightReference;
    }

    public Object clone()
    {
        MedMdrJoinRel clone = new MedMdrJoinRel(
            getCluster(),
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            leftOrdinal,
            rightReference);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  refine
        double rowCount = getRows();
        return planner.makeCost(rowCount, 0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        // REVIEW:  this assumes a one-to-many join
        return right.getRows();
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        MedMdrJoinRelImplementor joinImplementor =
            new MedMdrJoinRelImplementor(this);
        return joinImplementor.implement(implementor);
    }
}


// End MedMdrJoinRel.java
