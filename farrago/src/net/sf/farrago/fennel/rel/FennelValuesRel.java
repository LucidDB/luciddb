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

import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FennelValuesRel is Fennel implementation of {@link ValuesRel}. It corresponds
 * to <code>fennel::ValuesExecStream</code> via {@link FemValuesStreamDef}, and
 * guarantees the order of the tuples it produces, making it usable for such
 * purposes as the search input to an index scan, where order may matter for
 * both performance and correctness.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelValuesRel
    extends ValuesRelBase
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    private final boolean isVisibleInExplain;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelValuesRel. Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster .
     * @param rowType row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be of
     * same length, conforming to rowType
     * @param isVisibleInExplain whether the RelNode should appear in the
     * explain output
     */
    public FennelValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        boolean isVisibleInExplain)
    {
        super(
            cluster,
            rowType,
            tuples,
            new RelTraitSet(FENNEL_EXEC_CONVENTION));
        this.isVisibleInExplain = isVisibleInExplain;
    }

    /**
     * Creates a new FennelValuesRel. Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster .
     * @param rowType row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be of
     * same length, conforming to rowType
     */
    public FennelValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples)
    {
        super(
            cluster,
            rowType,
            tuples,
            new RelTraitSet(FENNEL_EXEC_CONVENTION));
        this.isVisibleInExplain = true;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemValuesStreamDef streamDef = repos.newFemValuesStreamDef();

        streamDef.setTupleBytesBase64(
            FennelRelUtil.convertTuplesToBase64String(rowType, tuples));

        return streamDef;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    public RelFieldCollation [] getCollations()
    {
        // TODO:  if tuples.size() == 1, say it's trivially sorted
        return RelFieldCollation.emptyCollationArray;
    }

    public boolean isVisibleInExplain()
    {
        return isVisibleInExplain;
    }
}

// End FennelValuesRel.java
