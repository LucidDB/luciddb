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

// REVIEW jvs 22-Mar-2006:  use generics to write a Java version
// of Fennel's OpaqueInteger?  Would have to use the boxed type
// underneath.

/**
 * FennelRelParamId is an opaque type representing the reservation of a {@link
 * net.sf.farrago.fennel.FennelDynamicParamId} during query planning. See <a
 * href="http://wiki.eigenbase.org/InternalDynamicParamScoping">the design
 * docs</a> for why this logical ID is needed in addition to
 * FennelDynamicParamId, which is the physical ID. A 64-bit integer is used
 * since a large number of these may be generated and then discarded during
 * query planning. (I hate to think about the implications of a planner that
 * would actually exhaust 32 bits, but still, one just can't be too safe.)
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRelParamId
{
    //~ Instance fields --------------------------------------------------------

    private final long id;

    //~ Constructors -----------------------------------------------------------

    public FennelRelParamId(long id)
    {
        this.id = id;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying long value
     */
    public long longValue()
    {
        return id;
    }

    // implement Object
    public boolean equals(Object other)
    {
        return ((other instanceof FennelRelParamId)
            && (((FennelRelParamId) other).longValue() == id));
    }

    // implement Object
    public int hashCode()
    {
        return (int) id;
    }

    // implement Object
    public String toString()
    {
        return Long.toString(id);
    }
}

// End FennelRelParamId.java
