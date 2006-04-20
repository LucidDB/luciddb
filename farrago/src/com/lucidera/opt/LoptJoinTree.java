/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.opt;

import org.eigenbase.rel.*;

import java.util.*;

/**
 * LoptJoinTree implements a utility class used to store a JoinRel tree and
 * the factors that make up the tree.  Because RelNodes can be duplicated in
 * a query when you have a self-join, factor ids are needed to distinguish
 * between the different join inputs that correspond to identical tables.
 * The class associates factor ids with a join tree, matching the order of
 * the factor ids with the order of those factors in the join tree.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptJoinTree
{
    private BinaryTree factorTree;
    private RelNode joinTree;

    // creates a jointree consisting of a single node
    public LoptJoinTree(RelNode joinTree, int factorId)
    {
        this.joinTree = joinTree;
        factorTree = new BinaryTree(factorId);
    }

    // associates the factor ids with a jointree
    public LoptJoinTree(RelNode joinTree, BinaryTree factorTree)
    {
        this.joinTree = joinTree;
        this.factorTree = factorTree;
    }

    // associates the factor ids with a jointree given the factors
    // corresponding to the left and right subtrees of the join
    public LoptJoinTree(
        RelNode joinTree,
        BinaryTree leftFactorTree, BinaryTree rightFactorTree)
    {
        factorTree = new BinaryTree(leftFactorTree, rightFactorTree);
        this.joinTree = joinTree;
    }
    
    public RelNode getJoinTree()
    {
        return joinTree;
    }

    public LoptJoinTree getLeft()
    {
        return new LoptJoinTree(
            ((JoinRel) joinTree).getLeft(), factorTree.getLeft());
    }

    public LoptJoinTree getRight()
    {
        return new LoptJoinTree(
            ((JoinRel) joinTree).getRight(), factorTree.getRight());
    }

    public BinaryTree getFactorTree()
    {
        return factorTree;
    }

    public void getTreeOrder(List<Integer> treeOrder)
    {
        factorTree.getTreeOrder(treeOrder);
    }
    
    // Simple binary tree class that stores an id in the leaf nodes
    private class BinaryTree
    {
        private int id;
        private BinaryTree left;
        private BinaryTree right;
        
        public BinaryTree(int rootId)
        {
            this.id = rootId;
            this.left = null;
            this.right = null;
        }
        
        public BinaryTree(BinaryTree left, BinaryTree right)
        {
            this.left = left;
            this.right = right;
        }
        
        public BinaryTree getLeft()
        {
            return left;
        }

        public BinaryTree getRight()
        {
            return right;
        }

        public void getTreeOrder(List<Integer> treeOrder)
        {
            if (left == null && right == null) {
                treeOrder.add(id);
            } else {
                left.getTreeOrder(treeOrder);
                right.getTreeOrder(treeOrder);
            }
        }
    }
}

// End LoptJoinTree.java
