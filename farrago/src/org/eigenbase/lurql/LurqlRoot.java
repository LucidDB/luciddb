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
 * LurqlRoot represents the parsed form of a simple root in the FROM clause of a
 * LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlRoot
    extends LurqlPathBranch
{
    //~ Instance fields --------------------------------------------------------

    private final String className;

    private final List<LurqlFilter> filterList;

    //~ Constructors -----------------------------------------------------------

    public LurqlRoot(
        String aliasName,
        String className,
        List<LurqlFilter> filterList,
        LurqlPathSpec pathSpec)
    {
        super(aliasName, pathSpec);
        this.className = className;
        this.filterList = Collections.unmodifiableList(filterList);
    }

    //~ Methods ----------------------------------------------------------------

    public String getClassName()
    {
        return className;
    }

    public List<LurqlFilter> getFilterList()
    {
        return filterList;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("class ");
        StackWriter.printSqlIdentifier(pw, className);
        unparseAlias(pw);
        unparseFilterList(pw, filterList);
        unparseThenSpec(pw);
    }
}

// End LurqlRoot.java
