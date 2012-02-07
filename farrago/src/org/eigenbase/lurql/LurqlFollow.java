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
package org.eigenbase.lurql;

import java.io.*;

import java.util.*;

import org.eigenbase.util.*;


/**
 * LurqlFollow represents a parsed FOLLOW clause in a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlFollow
    extends LurqlPathBranch
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String AF_ORIGIN_END = "origin end";

    public static final String AF_ORIGIN_CLASS = "origin class";

    public static final String AF_DESTINATION_END = "destination end";

    public static final String AF_DESTINATION_CLASS = "destination class";

    public static final String AF_COMPOSITE = "composite";

    public static final String AF_NONCOMPOSITE = "noncomposite";

    public static final String AF_ASSOCIATION = "association";

    public static final String AF_FORWARD = "forward";

    public static final String AF_BACKWARD = "backward";

    //~ Instance fields --------------------------------------------------------

    private final List<LurqlFilter> filterList;

    private final Map<String, String> associationFilters;

    //~ Constructors -----------------------------------------------------------

    public LurqlFollow(
        String aliasName,
        Map<String, String> associationFilters,
        List<LurqlFilter> filterList,
        LurqlPathSpec thenSpec)
    {
        super(aliasName, thenSpec);
        this.associationFilters =
            Collections.unmodifiableMap(
                associationFilters);
        this.filterList = Collections.unmodifiableList(filterList);
    }

    //~ Methods ----------------------------------------------------------------

    public Map<String, String> getAssociationFilters()
    {
        return associationFilters;
    }

    public List<LurqlFilter> getFilterList()
    {
        return filterList;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("follow");
        for (Object o : associationFilters.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            pw.print(" ");
            pw.print(entry.getKey());
            if (entry.getValue() != null) {
                pw.print(" ");
                StackWriter.printSqlIdentifier(
                    pw,
                    entry.getValue().toString());
            }
        }
        unparseAlias(pw);
        unparseFilterList(pw, filterList);
        unparseThenSpec(pw);
    }
}

// End LurqlFollow.java
