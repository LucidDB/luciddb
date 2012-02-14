/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.fennel.rel;

import java.util.ArrayList;
import java.util.List;

import net.sf.farrago.query.*;
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

        return new MethodCall(
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
        List<RelDataType> typeList = new ArrayList<RelDataType>();
        for (RelNode input : inputs) {
            RelDataType inputType = input.getRowType();
            typeList.add(inputType);
        }
        final RelDataType [] types =
            typeList.toArray(new RelDataType[typeList.size()]);
        return getCluster().getTypeFactory().createJoinType(types);
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelMultipleRel.java
