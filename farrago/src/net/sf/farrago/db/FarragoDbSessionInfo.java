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

import java.util.*;
import java.util.concurrent.*;

import net.sf.farrago.session.*;


/**
 * Implements the {@link FarragoSessionInfo} interface in the context of a
 * {@link FarragoDbSession}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDbSessionInfo
    implements FarragoSessionInfo
{

    //~ Instance fields --------------------------------------------------------

    private long id;
    private FarragoSession session;
    private FarragoDatabase database;
    private Map<Long, FarragoSessionExecutingStmtInfo> statements;
    // REVIEW mberkowitz 28-Mar-2006: maybe have 1 map id->info in
    // FarragoDatabase.

    //~ Constructors -----------------------------------------------------------

    FarragoDbSessionInfo(FarragoSession session, FarragoDatabase database)
    {
        this.id = database.getUniqueId();
        this.session = session;
        this.database = database;
        statements =
            new ConcurrentHashMap<Long, FarragoSessionExecutingStmtInfo>();
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoSession getSession()
    {
        return session;
    }

    FarragoDatabase getDatabase()
    {
        return database;
    }

    public long getId()
    {
        return id;
    }

    // implement FarragoSessionInfo
    public List<Long> getExecutingStmtIds()
    {
        Set<Long> s = statements.keySet();
        Long [] k = statements.keySet().toArray(new Long[s.size()]);
        return Collections.unmodifiableList(Arrays.asList(k));
    }

    // implement FarragoSessionInfo
    public FarragoSessionExecutingStmtInfo getExecutingStmtInfo(Long id)
    {
        return statements.get(id);
    }

    /**
     * Adds a running statement.
     *
     * @param info Info object for the running statement
     */
    public void addExecutingStmtInfo(FarragoSessionExecutingStmtInfo info)
    {
        statements.put(
            info.getId(),
            info);
    }

    /**
     * Removes a running statement.
     *
     * @param id Unique identifier of a running statement
     */
    public void removeExecutingStmtInfo(long id)
    {
        statements.remove(id);
    }
}

// End FarragoDbSessionInfo.java
