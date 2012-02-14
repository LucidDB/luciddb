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
package net.sf.farrago.namespace.util;

import java.util.*;

import net.sf.farrago.namespace.*;


/**
 * MedMetadataQueryImpl is a default implementation for {@link
 * FarragoMedMetadataQuery}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMetadataQueryImpl
    implements FarragoMedMetadataQuery
{
    //~ Instance fields --------------------------------------------------------

    private final Map<String, FarragoMedMetadataFilter> filterMap;

    private final Set<String> resultObjectTypes;

    //~ Constructors -----------------------------------------------------------

    public MedMetadataQueryImpl()
    {
        filterMap = new HashMap<String, FarragoMedMetadataFilter>();
        resultObjectTypes = new HashSet<String>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedMetadataQuery
    public Map<String, FarragoMedMetadataFilter> getFilterMap()
    {
        return filterMap;
    }

    // implement FarragoMedMetadataQuery
    public Set<String> getResultObjectTypes()
    {
        return resultObjectTypes;
    }
}

// End MedMetadataQueryImpl.java
