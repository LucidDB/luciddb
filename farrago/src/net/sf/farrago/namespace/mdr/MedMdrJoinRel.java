/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.namespace.mdr;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.stmt.*;

import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.netbeans.api.mdr.*;

import openjava.ptree.*;
import openjava.mop.*;

import java.util.*;
import javax.jmi.model.*;
import javax.jmi.reflect.*;

import java.util.List;

/**
 * MedMdrJoinRel is the relational expression corresponding to a join via
 * association to an MedMdrClassExtent on the right hand side.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRel extends JoinRel implements JavaRel
{
    private int leftOrdinal;
    
    private Reference rightReference;
    
    MedMdrJoinRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right,
        RexNode condition,
        int joinType,
        int leftOrdinal,
        Reference rightReference)
    {
        super(cluster,left,right,condition,joinType,Collections.EMPTY_SET);
        assert((joinType == JoinType.INNER) || (joinType == JoinType.LEFT));

        this.leftOrdinal = leftOrdinal;
        this.rightReference = rightReference;
    }

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
        return new MedMdrJoinRel(
            cluster,
            OptUtil.clone(left),
            OptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            leftOrdinal,
            rightReference);
    }

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }
    
    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        // TODO:  refine
        double rowCount = getRows();
        return planner.makeCost(
            rowCount,
            0,
            rowCount*getRowType().getFieldCount());
    }

    // implement SaffronRel
    public double getRows()
    {
        // REVIEW:  this assumes a one-to-many join
        return right.getRows();
    }

    // implement SaffronRel
    public ParseTree implement(JavaRelImplementor implementor)
    {
        MedMdrJoinRelImplementor joinImplementor =
            new MedMdrJoinRelImplementor(this);
        return joinImplementor.implement(implementor);
    }
}

// End MedMdrJoinRel.java
