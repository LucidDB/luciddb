/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.ArrayList;

import net.sf.farrago.type.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * Abstract base class for relational expressions which implement {@link
 * FennelRel} and have variable numbers of inputs.
 *
 * @author Jack Frost
 * @version $Id$
 * @since Feb 4, 2005
 */
public abstract class FennelMultipleRel
    extends AbstractRelNode
    implements FennelRel
{

    //~ Instance fields --------------------------------------------------------

    protected RelNode [] inputs;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelMultipleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param inputs array of inputs
     */
    protected FennelMultipleRel(
        RelOptCluster cluster,
        RelNode [] inputs)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION));
        this.inputs = inputs;
        assert inputs != null;
        for (int i = 0; i < inputs.length; i++) {
            assert inputs[i] != null;
        }
    }

    //~ Methods ----------------------------------------------------------------

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
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    // implement RelNode
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        final ExpressionList expressionList = new ExpressionList();

        for (int i = 0; i < inputs.length; i++) {
            RelNode input = inputs[i];
            Expression expr =
                (Expression) implementor.visitChild(this, i, input);
            expressionList.add(expr);
        }

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);

        return
            new MethodCall(
                stmt.getConnectionVariable(),
                "dummyArray",
                new ExpressionList(
                    new ArrayAllocationExpression(
                        OJClass.forClass(Object.class),
                        new ExpressionList(null),
                        new ArrayInitializer(expressionList))));
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
        final RelDataType [] types =
            (RelDataType []) typeList.toArray(new RelDataType[typeList.size()]);
        return getCluster().getTypeFactory().createJoinType(types);
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelMultipleRel.java
