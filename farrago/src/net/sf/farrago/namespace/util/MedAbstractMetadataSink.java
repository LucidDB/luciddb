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

import java.io.*;

import java.util.*;
import java.util.regex.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;


/**
 * MedAbstractMetadataSink is an abstract base class for implementations of the
 * {@link FarragoMedMetadataSink} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractMetadataSink
    implements FarragoMedMetadataSink
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoMedMetadataQuery query;
    private final FarragoTypeFactory typeFactory;
    private final Map<String, Pattern> patternMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new sink.
     *
     * @param query the query being processed; its filters will be implemented
     * by this sink
     * @param typeFactory factory for types written to this sink
     */
    protected MedAbstractMetadataSink(
        FarragoMedMetadataQuery query,
        FarragoTypeFactory typeFactory)
    {
        this.query = query;
        this.typeFactory = typeFactory;
        this.patternMap = new HashMap<String, Pattern>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Tests whether an object should be included in the query result.
     *
     * @param objectName name of object
     * @param typeName name of object type
     * @param qualifier if true, test whether object is a valid qualifier; if
     * false, test whether object itself should be included
     *
     * @return true if the inclusion test passes
     */
    protected boolean shouldInclude(
        String objectName,
        String typeName,
        boolean qualifier)
    {
        if (!qualifier) {
            if (!query.getResultObjectTypes().contains(typeName)) {
                return false;
            }
        }
        FarragoMedMetadataFilter filter =
            (FarragoMedMetadataFilter) query.getFilterMap().get(typeName);
        if (filter == null) {
            return true;
        }
        boolean included = false;
        if (filter.getRoster() != null) {
            if (filter.getRoster().contains(objectName)) {
                included = true;
            }
        } else {
            Pattern pattern = getPattern(filter.getPattern());
            included = pattern.matcher(objectName).matches();
        }
        if (filter.isExclusion()) {
            included = !included;
        }
        return included;
    }

    // implement FarragoMedMetadataSink
    public FarragoTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    Pattern getPattern(String likePattern)
    {
        Pattern pattern = patternMap.get(likePattern);
        if (pattern == null) {
            // TODO jvs 6-Aug-2005:  move this to a common location
            // where it can be used for LIKE evaluations in SQL expressions,
            // and enhance it with escapes, etc.
            StringWriter regex = new StringWriter();
            int n = likePattern.length();
            for (int i = 0; i < n; ++i) {
                char c = likePattern.charAt(i);
                switch (c) {
                case '%':
                    regex.write(".*");
                    break;
                case '_':
                    regex.write(".");
                    break;
                default:
                    regex.write((int) c);
                    break;
                }
            }
            pattern = Pattern.compile(regex.toString());
            patternMap.put(likePattern, pattern);
        }
        return pattern;
    }
}

// End MedAbstractMetadataSink.java
