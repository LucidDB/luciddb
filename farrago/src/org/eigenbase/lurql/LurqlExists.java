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


/**
 * LurqlExists represents an exists clause within a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlExists
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final List<String> selectList;

    private final LurqlPathSpec pathSpec;

    //~ Constructors -----------------------------------------------------------

    public LurqlExists(List<String> selectList, LurqlPathSpec pathSpec)
    {
        this.selectList = selectList;
        this.pathSpec = pathSpec;
    }

    //~ Methods ----------------------------------------------------------------

    public List<String> getSelectList()
    {
        return selectList;
    }

    public LurqlPathSpec getPathSpec()
    {
        return pathSpec;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("exists ");
        LurqlQuery.unparseSelectList(pw, selectList);
        pw.print(" in ");
        pathSpec.unparse(pw);
    }
}

// End LurqlExists.java
