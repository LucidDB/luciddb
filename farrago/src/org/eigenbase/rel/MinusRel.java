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
package org.eigenbase.rel;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * <code>MinusRel</code> returns the rows of its first input minus any matching
 * rows from its other inputs. If "all" is true, then multiset subtraction is
 * performed; otherwise, set subtraction is performed (implying no duplicates in
 * the results).
 *
 * @author jhyde
 * @version $Id$
 * @since 23 September, 2001
 */
public final class MinusRel
    extends SetOpRel
{
    //~ Constructors -----------------------------------------------------------

    public MinusRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        boolean all)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            inputs,
            all);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public double getRows()
    {
        // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
        double dRows = RelMetadataQuery.getRowCount(inputs[0]);
        for (int i = 1; i < inputs.length; i++) {
            dRows -= 0.5 * RelMetadataQuery.getRowCount(inputs[i]);
        }
        if (dRows < 0) {
            dRows = 0;
        }
        return dRows;
    }

    public MinusRel clone()
    {
        MinusRel clone =
            new MinusRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                all);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public MinusRel clone(RelNode [] inputs, boolean all)
    {
        MinusRel clone =
            new MinusRel(
                getCluster(),
                inputs,
                all);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End MinusRel.java
