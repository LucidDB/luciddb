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

import org.eigenbase.util.*;


/**
 * LurqlPathBranch represents a parsed path branch (either FROM, FOLLOW, or
 * RECURSIVELY) in a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlPathBranch
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final String aliasName;

    private final LurqlPathSpec thenSpec;

    //~ Constructors -----------------------------------------------------------

    protected LurqlPathBranch(String aliasName, LurqlPathSpec thenSpec)
    {
        this.aliasName = aliasName;
        this.thenSpec = thenSpec;
    }

    //~ Methods ----------------------------------------------------------------

    public String getAliasName()
    {
        return aliasName;
    }

    public LurqlPathSpec getThenSpec()
    {
        return thenSpec;
    }

    protected void unparseThenSpec(PrintWriter pw)
    {
        if (thenSpec != null) {
            pw.println();
            pw.print("then ");
            thenSpec.unparse(pw);
        }
    }

    protected void unparseAlias(PrintWriter pw)
    {
        if (aliasName != null) {
            pw.print(" as ");
            StackWriter.printSqlIdentifier(pw, aliasName);
        }
    }
}

// End LurqlPathBranch.java
