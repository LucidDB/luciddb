/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.query;

import net.sf.farrago.cwm.relational.*;

import net.sf.saffron.core.*;
import net.sf.saffron.ext.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

/**
 * An implementation of SaffronTable for accessing a view managed by Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoView extends FarragoQueryNamedColumnSet
{
    /**
     * Creates a new FarragoView object.
     *
     * @param cwmView catalog definition for view
     * @param rowType type for rows produced by view
     */
    FarragoView(
        CwmNamedColumnSet cwmView,
        SaffronType rowType)
    {
        super(cwmView,rowType);
    }

    public CwmView getCwmView()
    {
        return (CwmView) getCwmColumnSet();
    }
    
    // implement SaffronTable
    public SaffronRel toRel(VolcanoCluster cluster,SaffronConnection connection)
    {
        // REVIEW:  cache view definition?
        SaffronRel rel = getPreparingStmt().expandView(
            getCwmView().getQueryExpression().getBody());
        return OptUtil.createRenameRel(
            getRowType(),
            rel);
    }
}

// End FarragoView.java
