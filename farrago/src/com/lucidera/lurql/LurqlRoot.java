/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql;

import java.io.*;
import java.util.*;

import org.eigenbase.util.*;

/**
 * LurqlRoot represents the parsed form of a simple root in the FROM clause
 * of a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlRoot extends LurqlPathBranch
{
    private final String aliasName;

    private final String className;

    private final List filterList;

    public LurqlRoot(
        String aliasName, 
        String className,
        List filterList,
        LurqlPathSpec pathSpec)
    {
        super(pathSpec);
        this.aliasName = aliasName;
        this.className = className;
        this.filterList = Collections.unmodifiableList(filterList);
    }

    public String getAliasName()
    {
        return aliasName;
    }

    public String getClassName()
    {
        return className;
    }

    public List getFilterList()
    {
        return filterList;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("class ");
        StackWriter.printSqlIdentifier(pw, className);
        if (aliasName != null) {
            pw.print(" as ");
            StackWriter.printSqlIdentifier(pw, aliasName);
        }
        unparseFilterList(pw, filterList);
        unparseThenSpec(pw);
    }
}

// End LurqlRoot.java
