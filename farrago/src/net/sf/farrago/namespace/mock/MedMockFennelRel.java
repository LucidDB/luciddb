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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * MedMockFennelRel provides a mock implementation for {@link TableAccessRel}
 * with {@link FennelRel#FENNEL_EXEC_CONVENTION}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockFennelRel
    extends TableAccessRelBase
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    private MedMockColumnSet columnSet;

    //~ Constructors -----------------------------------------------------------

    MedMockFennelRel(
        MedMockColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            columnSet,
            connection);
        this.columnSet = columnSet;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemMockTupleStreamDef streamDef = repos.newFemMockTupleStreamDef();
        streamDef.setRowCount(columnSet.nRows);
        return streamDef;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // trivially sorted
        return new RelFieldCollation[] { new RelFieldCollation(0) };
    }

    // implement RelNode
    public MedMockFennelRel clone()
    {
        MedMockFennelRel clone =
            new MedMockFennelRel(
                columnSet,
                getCluster(),
                connection);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End MedMockFennelRel.java
