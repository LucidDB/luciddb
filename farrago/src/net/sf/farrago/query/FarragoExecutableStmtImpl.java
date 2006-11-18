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
import java.util.logging.*;

import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FarragoExecutableStmtImpl is an abstract base for implementations of
 * FarragoSessionExecutableStmt.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoExecutableStmtImpl
    extends FarragoCompoundAllocation
    implements FarragoSessionExecutableStmt
{

    //~ Instance fields --------------------------------------------------------

    private final boolean isDml;
    private final TableModificationRel.Operation tableModOp;
    private final RelDataType dynamicParamRowType;
    private final TableAccessMap tableAccessMap;
    protected static final Logger tracer =
        FarragoTrace.getClassTracer(FarragoExecutableStmtImpl.class);

    //~ Constructors -----------------------------------------------------------

    protected FarragoExecutableStmtImpl(
        RelDataType dynamicParamRowType,
        boolean isDml,
        TableModificationRel.Operation tableModOp,
        TableAccessMap tableAccessMap)
    {
        this.isDml = isDml;
        this.tableModOp = tableModOp;
        this.dynamicParamRowType = dynamicParamRowType;
        this.tableAccessMap = tableAccessMap;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public boolean isDml()
    {
        return isDml;
    }
    
    // implement FarragoSessionExecutableStmt
    public TableModificationRel.Operation getTableModOp()
    {
        return tableModOp;
    }

    // implement FarragoSessionExecutableStmt
    public RelDataType getDynamicParamRowType()
    {
        return dynamicParamRowType;
    }

    // implement FarragoSessionExecutableStmt
    public Set<String> getReferencedObjectIds()
    {
        return Collections.emptySet();
    }

    // implement FarragoSessionExecutableStmt
    public String getReferencedObjectModTime(String mofid)
    {
        return null;
    }

    // implement FarragoSessionExecutableStmt
    public TableAccessMap getTableAccessMap()
    {
        return tableAccessMap;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return Collections.emptyMap();
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getIterCalcTypeMap()
    {
        return Collections.EMPTY_MAP;
    }
}

// End FarragoExecutableStmtImpl.java
