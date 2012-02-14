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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * A relation which expands bitmap tuples of the form [keys, bitmaps] into
 * repetitious tuples of the form [keys]. One tuple will be output for each bit
 * set in an input bitmap.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsNormalizerRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    public LcsNormalizerRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
        repos = FennelRelUtil.getRepos(this);
    }

    //~ Methods ----------------------------------------------------------------

    // implement AbstractRelNode
    public LcsNormalizerRel clone()
    {
        LcsNormalizerRel clone =
            new LcsNormalizerRel(
                getCluster(),
                getChild());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement AbstractRelNode
    protected RelDataType deriveRowType()
    {
        RelDataType childType = getChild().getRowType();
        List<RelDataTypeField> childFields = childType.getFieldList();
        final int nKeys = childFields.size() - 3;
        final List<RelDataTypeField> keyFields = childFields.subList(0, nKeys);

        return getCluster().getTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return nKeys;
                }

                public String getFieldName(int index)
                {
                    return keyFields.get(index).getName();
                }

                public RelDataType getFieldType(int index)
                {
                    return keyFields.get(index).getType();
                }
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmNormalizerStreamDef normalizer =
            repos.newFemLbmNormalizerStreamDef();
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            normalizer);

        return normalizer;
    }
}

// End LcsNormalizerRel.java
