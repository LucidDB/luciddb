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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelSingleRel is a {@link FennelRel} corresponding to {@link SingleRel},
 * and which only takes a FennelRel as input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelSingleRel
    extends SingleRel
    implements FennelRel
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelSingleRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel
     */
    protected FennelSingleRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child);
    }

    /**
     * Creates a new FennelSingleRel object with specific traits.
     *
     * @param cluster RelOptCluster for this rel
     * @param traits traits for this rel
     * @param child input rel
     */
    protected FennelSingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child)
    {
        super(
            cluster,
            traits,
            child);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    /**
     * NOTE: this method is intentionally private because interactions between
     * FennelRels must be mediated by FarragoRelImplementor.
     *
     * @return this rel's input, which must already have been converted to a
     * FennelRel
     */
    private FennelRel getFennelInput()
    {
        return (FennelRel) getChild();
    }

    // default implementation for FennelRel
    public RelFieldCollation [] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(
            this,
            0,
            getChild());
    }
}

// End FennelSingleRel.java
