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
 * LurqlFollow represents a parsed FOLLOW clause in a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlFollow
    extends LurqlPathBranch
{

    //~ Static fields/initializers ---------------------------------------------

    public static final String AF_ORIGIN_END = "origin end";

    public static final String AF_ORIGIN_CLASS = "origin class";

    public static final String AF_DESTINATION_END = "destination end";

    public static final String AF_DESTINATION_CLASS = "destination class";

    public static final String AF_COMPOSITE = "composite";

    public static final String AF_NONCOMPOSITE = "noncomposite";

    public static final String AF_ASSOCIATION = "association";

    public static final String AF_FORWARD = "forward";

    public static final String AF_BACKWARD = "backward";

    //~ Instance fields --------------------------------------------------------

    private final List<LurqlFilter> filterList;

    private final Map<String, String> associationFilters;

    //~ Constructors -----------------------------------------------------------

    public LurqlFollow(
        String aliasName,
        Map<String, String> associationFilters,
        List<LurqlFilter> filterList,
        LurqlPathSpec thenSpec)
    {
        super(aliasName, thenSpec);
        this.associationFilters =
            Collections.unmodifiableMap(
                associationFilters);
        this.filterList = Collections.unmodifiableList(filterList);
    }

    //~ Methods ----------------------------------------------------------------

    public Map<String, String> getAssociationFilters()
    {
        return associationFilters;
    }

    public List<LurqlFilter> getFilterList()
    {
        return filterList;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("follow");
        for (Object o : associationFilters.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            pw.print(" ");
            pw.print(entry.getKey());
            if (entry.getValue() != null) {
                pw.print(" ");
                StackWriter.printSqlIdentifier(
                    pw,
                    entry.getValue().toString());
            }
        }
        unparseAlias(pw);
        unparseFilterList(pw, filterList);
        unparseThenSpec(pw);
    }
}

// End LurqlFollow.java
