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
package net.sf.farrago.db;

import java.util.Collections;
import java.util.List;

import net.sf.farrago.session.FarragoSessionExecutingStmtInfo;
import net.sf.farrago.session.FarragoSessionStmtContext;


/**
 * Implements the {@link FarragoSessionExecutingStmtInfo} interface
 * in the context of a {@link FarragoDbStmtContext}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDbSessionExecutingStmtInfo
    implements FarragoSessionExecutingStmtInfo
{
    //~ Instance fields -------------------------------------------------------

    private FarragoSessionStmtContext stmt;
    private long id;
    private String sql;
    private long startTime;
    private List<Object> parameters;
    private List<String> objectsInUse;

    //~ Constructors ----------------------------------------------------------

    FarragoDbSessionExecutingStmtInfo(
        FarragoSessionStmtContext stmt,
        long id,
        String sql,
        List<Object> parameters,
        List<String> objectsInUse)
    {
        this.stmt = stmt;
        this.id = id;
        this.sql = sql;
        this.startTime = System.currentTimeMillis();
        this.parameters = Collections.unmodifiableList(parameters);
        this.objectsInUse = Collections.unmodifiableList(objectsInUse);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionExecutingStmtInfo
    public FarragoSessionStmtContext getStmtContext()
    {
        return stmt;
    }


    // implement FarragoSessionExecutingStmtInfo
    public long getId()
    {
        return id;
    }

    // implement FarragoSessionExecutingStmtInfo
    public String getSql()
    {
        return sql;
    }

    // implement FarragoSessionExecutingStmtInfo
    public List<Object> getParameters()
    {
        return parameters;
    }

    // implement FarragoSessionExecutingStmtInfo
    public long getStartTime()
    {
        return startTime;
    }

    // implement FarragoSessionExecutingStmtInfo
    public List<String> getObjectsInUse()
    {
        return objectsInUse;
    }
}
