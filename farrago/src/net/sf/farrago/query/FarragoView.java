/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.query;

import net.sf.farrago.cwm.relational.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * An implementation of RelOptTable for accessing a view managed by Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoView extends FarragoQueryNamedColumnSet
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoView object.
     *
     * @param cwmView catalog definition for view
     * @param rowType type for rows produced by view
     */
    FarragoView(
        CwmNamedColumnSet cwmView,
        RelDataType rowType)
    {
        super(cwmView, rowType);
    }

    //~ Methods ---------------------------------------------------------------

    public CwmView getCwmView()
    {
        return (CwmView) getCwmColumnSet();
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        // REVIEW:  cache view definition?
        RelNode rel =
            getPreparingStmt().expandView(
                getCwmView().getQueryExpression().getBody());
        rel = RelOptUtil.createRenameRel(
            getRowType(),
            rel);
        rel = getPreparingStmt().flattenTypes(rel);
        return rel;
    }
}


// End FarragoView.java
