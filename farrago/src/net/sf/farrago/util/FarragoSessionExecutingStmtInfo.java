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
package net.sf.farrago.util;


/**
 * FarragoSessionExecuctingStmtInfo contains information about executing statements.
 */
public class FarragoSessionExecutingStmtInfo
{
    //~ Instance fields -------------------------------------------------------

    private Integer id;
    private String sql;
    private long startTime;
    private Object [] parameters;
    private String[] objectsInUse;

    //~ Constructors ----------------------------------------------------------

    public FarragoSessionExecutingStmtInfo(
        String sql,
        Object [] parameters,
        String[] objectsInUse)
    {
        this.sql = sql;
        this.startTime = System.currentTimeMillis();
        this.parameters = parameters;
        this.objectsInUse = objectsInUse;
        this.id = this.hashCode();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the unique identifier for this executing statement.
     * @return
     */
    public Integer getId()
    {
        return id;
    }

    /**
     * Returns the SQL statement being executed.
     * @return SQL statement
     */
    public String getSql()
    {
        return sql;
    }

    /**
     * Returns any dynamic parameters used to execute this statement.
     * @return Object[] of dynamic parameters to the statement
     */
    public Object [] getParameters()
    {
        return parameters;
    }

    /**
     * Returns time the statement began executing, in ms.
     * @return Start time in ms
     */
    public long getStartTime()
    {
        return startTime;
    }

    /**
     * Returns an array of catalog object mofIds in use by this statement.
     * @return String[] of catalog object mofIds
     */
    public String[] getObjectsInUse()
    {
        return objectsInUse;
    }
}
