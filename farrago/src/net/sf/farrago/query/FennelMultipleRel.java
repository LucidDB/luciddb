/*
// Farrago is a relational database management system.
// Copyright (C) 2005-2005 John V. Sichi.
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

import net.sf.farrago.type.FarragoTypeFactory;
import openjava.ptree.Expression;
import openjava.ptree.ExpressionList;
import openjava.ptree.MethodCall;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.reltype.RelDataType;

import java.util.ArrayList;

/**
 * Abstract base class for relational expressions which implement 
 * {@link FennelRel} and have variable numbers of inputs.
 *
 * @author Jack Frost
 * @since Feb 4, 2005
 * @version $Id$
 */
public abstract class FennelMultipleRel 
    extends AbstractRelNode implements FennelRel
{
    //~ Instance fields -------------------------------------------------------

    protected RelNode[] inputs;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelDoubleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param inputs array of inputs
     */
    protected FennelMultipleRel(
        RelOptCluster cluster,
        RelNode[] inputs)
    {
        super(cluster);
        this.inputs = inputs;
        assert inputs != null;
        for (int i = 0; i < inputs.length; i++) {
            assert inputs[i] != null;
//            assert inputs[i] instanceof FennelRel;
//            assert (FennelRelUtil.getPreparingStmt((FennelRel) inputs[i]) == preparingStmt);
        }
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelNode
    public RelNode [] getInputs()
    {
        return inputs;
    }

    // implement RelNode
    public void replaceInput(
        int ordinalInParent,
        RelNode p)
    {
        inputs[ordinalInParent] = p;
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) cluster.typeFactory;
    }

    // implement RelNode
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        final ExpressionList expressionList = new ExpressionList();
        for (int i = 0; i < inputs.length; i++) {
            RelNode input = inputs[i];
            Expression expr = 
                (Expression) implementor.visitChild(this, 0, input);
            expressionList.add(expr);            
        }

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);

        return new MethodCall(
            stmt.getConnectionVariable(),
            "dummyPair",
            expressionList);
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        ArrayList typeList = new ArrayList();
        for (int i = 0; i < inputs.length; i++) {
            RelNode input = inputs[i];
            RelDataType inputType = input.getRowType();
            typeList.add(inputType);
        }
        final RelDataType[] types = 
            (RelDataType[]) typeList.toArray(new RelDataType[typeList.size()]);
        return cluster.typeFactory.createJoinType(types);
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelMultipleRel.java
