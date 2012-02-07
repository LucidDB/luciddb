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
package net.sf.farrago.ddl.gen;

import java.util.*;


/**
 * Working set for DdlGenerator. Maintains list of DDL statement lines.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class GeneratedDdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final List<String> ddl = new ArrayList<String>();
    private final boolean replace;
    private final String newName;
    private boolean topLevel;

    //~ Constructors -----------------------------------------------------------

    public GeneratedDdlStmt()
    {
        this(null, true);
    }

    public GeneratedDdlStmt(boolean replace)
    {
        this(null, replace);
    }

    public GeneratedDdlStmt(String newName)
    {
        this(newName, true);
    }

    private GeneratedDdlStmt(String newName, boolean replace)
    {
        this.replace = replace;
        this.newName = newName;
        clear();
    }

    //~ Methods ----------------------------------------------------------------

    public void clear()
    {
        ddl.clear();
        this.topLevel = true;
    }

    public void addStmt(String stmt)
    {
        ddl.add(stmt);
    }

    public boolean isReplace()
    {
        return replace;
    }

    public String getNewName()
    {
        return newName;
    }

    /**
     * Indicates whether the element is a top-level element, that is, it
     * requires its own DDL statement.
     *
     * <p>For example, a regular index is top-level but a clustered index is not
     * (it lives inside a CREATE TABLE statement).
     *
     * <p>The {@link #clear()} method resets the <code>topLevel</code> attribute
     * to <code>true</code>.
     *
     * @param topLevel Whether element has its own DDL statement
     */
    public void setTopLevel(boolean topLevel)
    {
        this.topLevel = topLevel;
    }

    /**
     * Returns whether the element is a top-level element.
     *
     * @return whether the element is a top-level element
     *
     * @see #setTopLevel(boolean)
     */
    public boolean isTopLevel()
    {
        return topLevel;
    }

    /**
     * Returns the list of generated DDL strings.
     *
     * @return list of DDL statements
     */
    public List<String> getStatementList()
    {
        return ddl;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (String s : ddl) {
            sb.append(s);
        }
        return sb.toString();
    }
}

// End GeneratedDdlStmt.java
