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
