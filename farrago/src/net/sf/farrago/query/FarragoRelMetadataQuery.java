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
package net.sf.farrago.query;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;


/**
 * FarragoRelMetadataQuery defines the relational expression metadata queries
 * specific to Farrago.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FarragoRelMetadataQuery
    extends RelMetadataQuery
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Determines whether a physical expression can be restarted. For leaves,
     * default implementation is true; for non-leaves, default implementation is
     * conjunction of children.
     *
     * @param rel the relational expression
     *
     * @return true if restart is possible
     */
    public static boolean canRestart(RelNode rel)
    {
        return (Boolean) rel.getCluster().getMetadataProvider().getRelMetadata(
            rel,
            "canRestart",
            null);
    }
}

// End FarragoRelMetadataQuery.java
