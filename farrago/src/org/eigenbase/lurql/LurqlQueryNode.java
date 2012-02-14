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
 * LurqlQueryNode is an abstract base class representing a node in a LURQL parse
 * tree.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlQueryNode
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts this node to text.
     *
     * @param pw the PrintWriter on which to unparse; must have an underlying
     * {@link StackWriter} to interpret indentation
     */
    public abstract void unparse(PrintWriter pw);

    // override Object
    public String toString()
    {
        StringWriter sw = new StringWriter();
        StackWriter stack = new StackWriter(sw, StackWriter.INDENT_SPACE4);
        PrintWriter pw = new PrintWriter(stack);
        unparse(pw);
        pw.close();
        return sw.toString();
    }

    protected void unparseFilterList(
        PrintWriter pw,
        List<LurqlFilter> filterList)
    {
        if (!filterList.isEmpty()) {
            pw.println();
            pw.println("where");
            pw.write(StackWriter.INDENT);
            int k = 0;
            for (LurqlFilter filter : filterList) {
                if (k++ > 0) {
                    pw.println(" and");
                }
                filter.unparse(pw);
            }
            pw.write(StackWriter.OUTDENT);
        }
    }
}

// End LurqlQueryNode.java
