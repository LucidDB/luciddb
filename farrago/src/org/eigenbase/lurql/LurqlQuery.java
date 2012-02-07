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
 * LurqlQuery represents the parsed form of a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlQuery
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final List<String> selectList;

    private final LurqlQueryNode root;

    //~ Constructors -----------------------------------------------------------

    public LurqlQuery(List<String> selectList, LurqlQueryNode root)
    {
        this.selectList = Collections.unmodifiableList(selectList);
        this.root = root;
    }

    //~ Methods ----------------------------------------------------------------

    public List<String> getSelectList()
    {
        return selectList;
    }

    public LurqlQueryNode getRoot()
    {
        return root;
    }

    static void unparseSelectList(PrintWriter pw, List<String> selectList)
    {
        int k = 0;
        for (String id : selectList) {
            if (k++ > 0) {
                pw.print(", ");
            }
            if (id.equals("*")) {
                pw.print(id);
            } else {
                StackWriter.printSqlIdentifier(pw, id);
            }
        }
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("select ");
        unparseSelectList(pw, selectList);
        pw.println();
        pw.println("from");
        root.unparse(pw);
        pw.println(";");
    }
}

// End LurqlQuery.java
