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
package org.eigenbase.dmv;

import java.util.*;

import javax.jmi.reflect.*;

import org.eigenbase.jmi.*;


/**
 * DmvResponse represents the result of a DMV query.
 *
 * @author John Sichi
 * @version $Id$
 */
public class DmvResponse
{
    //~ Instance fields --------------------------------------------------------

    private final Collection<RefObject> searchResult;

    private final JmiDependencyGraph transformationResult;

    //~ Constructors -----------------------------------------------------------

    public DmvResponse(
        Collection<RefObject> searchResult,
        JmiDependencyGraph transformationResult)
    {
        this.searchResult = searchResult;
        this.transformationResult = transformationResult;
    }

    //~ Methods ----------------------------------------------------------------

    public Collection<RefObject> getSearchResult()
    {
        return searchResult;
    }

    public JmiDependencyGraph getTransformationResult()
    {
        return transformationResult;
    }
}

// End DmvResponse.java
