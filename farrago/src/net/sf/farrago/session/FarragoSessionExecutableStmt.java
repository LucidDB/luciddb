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
package net.sf.farrago.session;

import java.sql.*;
import java.util.*;

import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FarragoSessionExecutableStmt represents the executable output of
 * FarragoPreparingStmt processing.  Instances must be reentrant, so that
 * multiple threads can be executed simultaneously (each with a private
 * FarragoSessionRuntimeContext).
 *
 *<p>
 *
 * NOTE: FarragoSessionExecutableStmt implementations must be kept as lean as
 * possible for optimal caching (we want memory usage to be minimum, and usage
 * estimation to be as accurate as possible).  In particular, they must have no
 * references to information needed only during preparation; all of that should
 * be made available to the garbage collector.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionExecutableStmt extends FarragoAllocationOwner
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Executes this statement.
     *
     * @param runtimeContext context in which to execute
     *
     * @return ResultSet produced by statement
     */
    public ResultSet execute(FarragoSessionRuntimeContext runtimeContext);

    /**
     * @return type descriptor for rows produced by this stmt
     */
    public RelDataType getRowType();

    /**
     * @return type descriptor for row of dynamic parameters expected
     * by this stmt
     */
    public RelDataType getDynamicParamRowType();

    /**
     * @return true if this statement is DML; false if a query
     */
    public boolean isDml();

    /**
     * @return approximate total number of bytes used by this
     * statement's in-memory representation
     */
    public long getMemoryUsage();

    /**
     * @return Set of MOFID's of objects accessed when this stmt
     * is executed
     */
    public Set getReferencedObjectIds();

    /**
     * @return map of access modes for all tables referenced
     */
    public TableAccessMap getTableAccessMap();
}


// End FarragoSessionExecutableStmt.java
