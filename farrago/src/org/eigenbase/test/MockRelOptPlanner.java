/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package org.eigenbase.test;

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.oj.rel.*;

import java.util.*;

/**
 * MockRelOptPlanner is a mock implementation of the {@link RelOptPlanner}
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MockRelOptPlanner implements RelOptPlanner
{
    private RelNode root;
    
    // implement RelOptPlanner
    public void setRoot(RelNode rel)
    {
        this.root = rel;
    }
    
    // implement RelOptPlanner
    public RelNode getRoot()
    {
        return root;
    }

    // implement RelOptPlanner
    public boolean addRelTraitDef(RelTraitDef relTraitDef)
    {
        return false;
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        return false;
    }

    // implement RelOptPlanner
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelOptPlanner chooseDelegate()
    {
        return this;
    }

    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        return root;
    }

    // implement RelOptPlanner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeHugeCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeInfiniteCost()
    {
        return new MockRelOptCost();
    }
    
    // implement RelOptPlanner
    public RelOptCost makeTinyCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeZeroCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost getCost(RelNode rel)
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    // implement RelOptPlanner
    public void registerSchema(RelOptSchema schema)
    {
    }

    // implement RelOptPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return null;
    }

    // implement RelOptPlanner
    public void addListener(RelOptListener newListener)
    {
    }
}

// End MockRelOptPlanner.java
