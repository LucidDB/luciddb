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
package net.sf.farrago.namespace;

import java.util.*;


/**
 * FarragoMedMetadataFilter represents a filter on a {@link
 * FarragoMedMetadataQuery}. A filter may be expressed either as an explicit
 * name roster or as a name pattern (but not both at once).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataFilter
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return true if objects matching filter are to be excluded from query
     * results; false if only objects matching filter are to be included in the
     * results
     */
    public boolean isExclusion();

    /**
     * @return Set<String> representing filter membership, or null for a pattern
     * filter
     */
    public Set getRoster();

    /**
     * @return LIKE pattern, or null for a roster filter
     */
    public String getPattern();
}

// End FarragoMedMetadataFilter.java
