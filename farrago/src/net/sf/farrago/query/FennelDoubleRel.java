/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.core.*;
import net.sf.saffron.util.*;

import openjava.ptree.*;

/**
 * FennelDoubleRel is a {@link FennelRel} which takes two FennelRels as inputs.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FennelDoubleRel extends SaffronRel implements FennelRel
{
    SaffronRel left;
    SaffronRel right;
    
    /**
     * Creates a new FennelDoubleRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param left left input
     * @param right right input
     */
    protected FennelDoubleRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right)
    {
        super(cluster);
        this.left = left;
        this.right = right;
    }

    // implement SaffronRel
    public SaffronRel [] getInputs()
    {
        return new SaffronRel [] { left,right };
    }

    // implement SaffronRel
    public void replaceInput(int ordinalInParent,SaffronRel p)
    {
        switch (ordinalInParent) {
        case 0:
            left = p;
            break;
        case 1:
            right = p;
            break;
        default:
            throw Util.newInternal();
        }
    }

    // implement FennelRel
    public FarragoPreparingStmt getPreparingStmt()
    {
        return getFennelLeftInput().getPreparingStmt();
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) cluster.typeFactory;
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1);
        Expression expr1 =
            (Expression) implementor.implementChild(this,0,left);
        Expression expr2 =
            (Expression) implementor.implementChild(this,1,right);
        return new MethodCall(
            getPreparingStmt().getConnectionVariable(),
            "dummyPair",
            new ExpressionList(expr1,expr2));
    }

    // implement SaffronRel
    protected SaffronType deriveRowType()
    {
        SaffronType leftType = left.getRowType();
        SaffronType rightType = right.getRowType();
        return cluster.typeFactory.createJoinType(
            new SaffronType [] { leftType,rightType });
    }
    
    /**
     * .
     *
     * @return catalog for object definitions
     */
    FarragoCatalog getCatalog()
    {
        return getPreparingStmt().getCatalog();
    }

    /**
     * NOTE:  this method is intentionally private because interactions
     * between FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's left input, which must already have been
     * converted to a FennelRel
     */
    private FennelRel getFennelLeftInput()
    {
        return (FennelRel) left;
    }

    /**
     * NOTE:  this method is intentionally private because interactions
     * between FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's right input, which must already have been
     * converted to a FennelRel
     */
    private FennelRel getFennelRightInput()
    {
        return (FennelRel) right;
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelDoubleRel.java
