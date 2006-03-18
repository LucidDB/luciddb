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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FarragoExecutableStmtImpl is an abstract base for implementations of
 * FarragoSessionExecutableStmt.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoExecutableStmtImpl extends FarragoCompoundAllocation
    implements FarragoSessionExecutableStmt
{
    //~ Instance fields -------------------------------------------------------

    private final boolean isDml;
    private final RelDataType dynamicParamRowType;
    private final TableAccessMap tableAccessMap;

    //~ Constructors ----------------------------------------------------------

    protected FarragoExecutableStmtImpl(
        RelDataType dynamicParamRowType,
        boolean isDml,
        TableAccessMap tableAccessMap)
    {
        this.isDml = isDml;
        this.dynamicParamRowType = dynamicParamRowType;
        this.tableAccessMap = tableAccessMap;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public boolean isDml()
    {
        return isDml;
    }

    // implement FarragoSessionExecutableStmt
    public RelDataType getDynamicParamRowType()
    {
        return dynamicParamRowType;
    }

    // implement FarragoSessionExecutableStmt
    public Set getReferencedObjectIds()
    {
        return Collections.EMPTY_SET;
    }
    
    // implement FarragoSessionExecutableStmt
    public TableAccessMap getTableAccessMap()
    {
        return tableAccessMap;
    }
}


// End FarragoExecutableStmtImpl.java
