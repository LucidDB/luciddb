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
 * LurqlPathSpec represents the parsed form of a path specification in a LURQL
 * query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPathSpec
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final List<LurqlQueryNode> branches;

    private final LurqlPathSpec gatherThen;

    private final boolean gatherParent;

    //~ Constructors -----------------------------------------------------------

    public LurqlPathSpec(
        List<LurqlQueryNode> branches,
        LurqlPathSpec gatherThen,
        boolean gatherParent)
    {
        this.branches = Collections.unmodifiableList(branches);
        this.gatherThen = gatherThen;
        this.gatherParent = gatherParent;
    }

    //~ Methods ----------------------------------------------------------------

    public List<LurqlQueryNode> getBranches()
    {
        return branches;
    }

    public boolean isGather()
    {
        return gatherThen != null;
    }

    public LurqlPathSpec getGatherThenSpec()
    {
        return gatherThen;
    }

    public boolean isGatherParent()
    {
        return gatherParent;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.println("(");
        pw.write(StackWriter.INDENT);
        int k = 0;
        for (LurqlQueryNode branch : branches) {
            if (k++ > 0) {
                pw.println("union");
            }
            branch.unparse(pw);
            pw.println();
        }
        pw.write(StackWriter.OUTDENT);
        pw.print(")");
        if (isGather()) {
            pw.print(" gather ");
            if (isGatherParent()) {
                pw.print("with parent ");
            }
            pw.print("then ");
            gatherThen.unparse(pw);
        }
    }
}

// End LurqlPathSpec.java
