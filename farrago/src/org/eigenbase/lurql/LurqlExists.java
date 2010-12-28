/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
