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

import net.sf.farrago.runtime.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;

import java.sql.*;

/**
 * FarragoExecutableStmt represents the executable output of
 * FarragoPreparingStmt processing.  Instances must be reentrant, so that
 * multiple threads can be executed simultaneously (each with a private
 * FarragoRuntimeContext).
 *
 *<p>
 *
 * NOTE: FarragoExecutableStmt implementations must kept as lean as possible
 * for optimal caching (we want memory usage to be minimum, and usage
 * estimation to be as accurate as possible).  In particular, they must have no
 * references to information needed only during preparation; all of that should
 * be made available to the garbage collector.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoExecutableStmt
    extends FarragoAllocationOwner
{
    /**
     * Execute this statement.
     *
     * @param runtimeContext context in which to execute
     *
     * @return ResultSet produced by statement
     */
    public ResultSet execute(
        FarragoRuntimeContext runtimeContext);

    /**
     * .
     *
     * @return type descriptor for rows produced by this stmt
     */
    public SaffronType getRowType();

    /**
     * .
     *
     * @return type descriptor for row of dynamic parameters expected
     * by this stmt
     */
    public SaffronType getDynamicParamRowType();
    
    /**
     * .
     *
     * @return true if this statement is DML; false if a query
     */
    public boolean isDml();

    /**
     * .
     *
     * @return approximate total number of bytes used by this
     * statement's in-memory representation
     */
    public long getMemoryUsage();
}

// End FarragoExecutableStmt.java
