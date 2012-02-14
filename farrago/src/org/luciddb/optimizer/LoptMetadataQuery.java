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
package org.luciddb.optimizer;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;


/**
 * LoptMetadataQuery defines the relational expression metadata queries which
 * are custom to LucidDB's optimizer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LoptMetadataQuery
    extends RelMetadataQuery
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs, given a set of filters which will be applied to its
     * output. The default implementation assumes that the filters cannot be
     * used to reduce the processing cost, and will be evaluated above by a
     * calculator; so the result is the number of rows produced by rel plus the
     * cumulative cost of its inputs. For expressions such as row scans, more
     * efficient filter processing may be possible.
     *
     * @param rel the relational expression
     * @param filters filters to be applied
     *
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    public static Double getCostWithFilters(RelNode rel, RexNode filters)
    {
        return (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
            rel,
            "getCostWithFilters",
            new Object[] { filters });
    }

    /**
     * Like {@link RelMetadataQuery#getColumnOrigins}, for a given output column
     * of an expression, determines all columns of underlying tables which
     * contribute to result values. The difference is if the column is derived
     * from a complex {@link RelNode}, then null is returned instead.
     *
     * <p>A 'complex RelNode' is a RelNode that we do not push {@link
     * org.eigenbase.rel.rules.SemiJoinRel}s past.
     *
     * @param rel the relational expression
     * @param iOutputColumn 0-based ordinal for output column of interest
     *
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all) or the column is derived from a complex RelNode.
     */
    public static Set<RelColumnOrigin> getSimpleColumnOrigins(
        RelNode rel,
        int iOutputColumn)
    {
        final Object o =
            rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getSimpleColumnOrigins",
                new Object[] { iOutputColumn });
        return (Set<RelColumnOrigin>) o;
    }
}

// End LoptMetadataQuery.java
