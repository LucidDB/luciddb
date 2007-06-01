/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import java.util.*;


/**
 * FarragoSessionExecuctingStmtInfo contains information about executing
 * statements.
 */
public interface FarragoSessionExecutingStmtInfo
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the executing statement itself, as a FarragoSessionStmtContext
     */
    FarragoSessionStmtContext getStmtContext();

    /**
     * Returns the unique identifier for this executing statement.
     *
     * @return Unique statement ID
     */
    long getId();

    /**
     * Returns the SQL statement being executed.
     *
     * @return SQL statement
     */
    String getSql();

    /**
     * Returns any dynamic parameters used to execute this statement.
     *
     * @return List of dynamic parameters to the statement
     */
    List<Object> getParameters();

    /**
     * Returns time the statement began executing, in ms.
     *
     * @return Start time in ms
     */
    long getStartTime();

    /**
     * Returns an array of catalog object mofIds in use by this statement.
     *
     * @return List of catalog object mofIds
     */
    List<String> getObjectsInUse();
}

// End FarragoSessionExecutingStmtInfo.java
