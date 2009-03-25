/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
