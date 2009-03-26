/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.fennel.rel;

import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FennelDoubleRel is a {@link FennelRel} which takes two FennelRels as inputs.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelDoubleRel
    extends AbstractRelNode
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    protected RelNode left;
    protected RelNode right;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelDoubleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param left left input
     * @param right right input
     */
    protected FennelDoubleRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION));
        this.left = left;
        this.right = right;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public RelNode [] getInputs()
    {
        return new RelNode[] { left, right };
    }

    // implement RelNode
    public void replaceInput(
        int ordinalInParent,
        RelNode p)
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
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    // implement RelNode
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        Expression expr1 = (Expression) implementor.visitChild(this, 0, left);
        Expression expr2 = (Expression) implementor.visitChild(this, 1, right);
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);
        return new MethodCall(
            stmt.getConnectionVariable(),
            "dummyPair",
            new ExpressionList(expr1, expr2));
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        RelDataType leftType = left.getRowType();
        RelDataType rightType = right.getRowType();
        return getCluster().getTypeFactory().createJoinType(
            new RelDataType[] { leftType, rightType });
    }

    /**
     * NOTE: this method is intentionally private because interactions between
     * FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's left input, which must already have been converted to
     * a FennelRel
     */
    private FennelRel getFennelLeftInput()
    {
        return (FennelRel) left;
    }

    /**
     * NOTE: this method is intentionally private because interactions between
     * FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's right input, which must already have been converted to
     * a FennelRel
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
