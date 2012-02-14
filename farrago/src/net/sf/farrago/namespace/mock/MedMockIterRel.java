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
package net.sf.farrago.namespace.mock;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql2rel.*;


/**
 * MedMockIterRel provides a mock implementation for {@link TableAccessRel} with
 * {@link org.eigenbase.relopt.CallingConvention#ITERATOR}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockIterRel
    extends TableAccessRelBase
    implements JavaRel,
        RelStructuredTypeFlattener.SelfFlatteningRel
{
    //~ Instance fields --------------------------------------------------------

    private MedMockColumnSet columnSet;

    //~ Constructors -----------------------------------------------------------

    MedMockIterRel(
        MedMockColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            columnSet,
            connection);
        this.columnSet = columnSet;
    }

    //~ Methods ----------------------------------------------------------------

    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                outputRowType,
                implementor.getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(MedMockTupleIter.class),
                new ExpressionList(
                    newRowExp,
                    Literal.makeLiteral(columnSet.nRows)));

        return iterExp;
    }

    // implement RelNode
    public MedMockIterRel clone()
    {
        MedMockIterRel clone =
            new MedMockIterRel(
                columnSet,
                getCluster(),
                connection);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelStructuredTypeFlattener.SelfFlatteningRel
    public void flattenRel(RelStructuredTypeFlattener flattener)
    {
        flattener.rewriteGeneric(this);
    }
}

// End MedMockIterRel.java
