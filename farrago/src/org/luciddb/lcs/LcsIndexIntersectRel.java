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
 * LcsIndexIntersectRel is a relation for intersecting the results of two index
 * scans. The input to this relation must be more than one.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexIntersectRel
    extends LcsIndexBitOpRel
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsIndexIntersectRel object.
     */
    public LcsIndexIntersectRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super(cluster, inputs, lcsTable, startRidParamId, rowLimitParamId);
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public LcsIndexIntersectRel clone()
    {
        return new LcsIndexIntersectRel(
            getCluster(),
            RelOptUtil.clone(getInputs()),
            lcsTable,
            startRidParamId,
            rowLimitParamId);
    }

    // implement RelNode
    public double getRows()
    {
        // get the minimum number of rows across the children and then make
        // the cost inversely proportional to the number of children
        double minChildRows = 0;
        for (int i = 0; i < inputs.length; i++) {
            if ((minChildRows == 0) || (inputs[i].getRows() < minChildRows)) {
                minChildRows = RelMetadataQuery.getRowCount(inputs[i]);
            }
        }
        return minChildRows / inputs.length;
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmIntersectStreamDef intersectStream =
            lcsTable.getIndexGuide().newBitmapIntersect(
                implementor.translateParamId(startRidParamId),
                implementor.translateParamId(rowLimitParamId));

        setBitOpChildStreams(implementor, intersectStream);

        return intersectStream;
    }
}

// End LcsIndexIntersectRel.java
