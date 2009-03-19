/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
