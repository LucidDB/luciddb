/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

    private List<String> ddl = new ArrayList<String>();
    private boolean replace = true;
    private String newName;

    //~ Constructors -----------------------------------------------------------

    public GeneratedDdlStmt()
    {
    }

    public GeneratedDdlStmt(boolean replace)
    {
        this.replace = replace;
    }

    public GeneratedDdlStmt(String newName)
    {
        this.replace = true;
        this.newName = newName;
    }

    //~ Methods ----------------------------------------------------------------

    public void clear()
    {
        ddl.clear();
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

    public void setNewName(String newName)
    {
        this.newName = newName;
    }

    public Iterator iterator()
    {
        return ddl.iterator();
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        Iterator i = iterator();
        while (i.hasNext()) {
            sb.append((String) i.next());
        }
        return sb.toString();
    }
}

// End GeneratedDdlStmt.java
