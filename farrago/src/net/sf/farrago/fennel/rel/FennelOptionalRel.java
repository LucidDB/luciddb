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

import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelOptionalRel is a {@link FennelRel} which either takes zero inputs or
 * takes a single FennelRel as input.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class FennelOptionalRel
    extends FennelSingleRel
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelOptionalRel object with an input rel.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel
     */
    protected FennelOptionalRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    /**
     * Creates a new FennelOptionalRel object without an input rel.
     *
     * @param cluster RelOptCluster for this rel
     */
    protected FennelOptionalRel(
        RelOptCluster cluster)
    {
        super(cluster, null);
    }

    //~ Methods ----------------------------------------------------------------

    // override SingleRel
    public RelNode [] getInputs()
    {
        if (getChild() != null) {
            return super.getInputs();
        }
        return AbstractRelNode.emptyArray;
    }

    // override SingleRel
    public void childrenAccept(RelVisitor visitor)
    {
        if (getChild() != null) {
            super.childrenAccept(visitor);
        }
    }

    // override FennelSingleRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        if (getChild() != null) {
            return super.implementFennelChild(implementor);
        }
        return Literal.constantNull();
    }

    // override FennelSingleRel
    public double getRows()
    {
        if (getChild() != null) {
            return super.getRows();
        }
        return 1.0;
    }
}

// End FennelOptionalRel.java
