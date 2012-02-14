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
package org.luciddb.lcs;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LcsIndexMinusRel is a relation for "subtracting" the 2nd through Nth inputs
 * from the first input. The input to this relation must be more than one.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexMinusRel
    extends LcsIndexBitOpRel
{
    //~ Constructors -----------------------------------------------------------

    public LcsIndexMinusRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super(cluster, inputs, lcsTable, startRidParamId, rowLimitParamId);
    }

    //~ Methods ----------------------------------------------------------------

    public LcsIndexMinusRel clone()
    {
        return new LcsIndexMinusRel(
            getCluster(),
            RelOptUtil.clone(getInputs()),
            lcsTable,
            startRidParamId,
            rowLimitParamId);
    }

    // implement RelNode
    public double getRows()
    {
        // get the number of rows from the first child and then reduce it
        // by the number of children
        double anchorRows = RelMetadataQuery.getRowCount(inputs[0]);
        return anchorRows / inputs.length;
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmMinusStreamDef minusStream =
            lcsTable.getIndexGuide().newBitmapMinus(
                implementor.translateParamId(startRidParamId),
                implementor.translateParamId(rowLimitParamId),
                inputs[0]);

        setBitOpChildStreams(implementor, minusStream);

        return minusStream;
    }
}

// End LcsIndexMinusRel.java
