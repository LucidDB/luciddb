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
package net.sf.farrago.db;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;


/**
 * FarragoDbSessionFactory is a default implementation for the
 * {@link net.sf.farrago.session.FarragoSessionFactory} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbSessionFactory implements FarragoSessionFactory
{
    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionFactory
    public FarragoSession newSession(
        String url,
        Properties info)
    {
        return new FarragoDbSession(url, info, this);
    }

    // implement FarragoSessionFactory
    public FennelCmdExecutor newFennelCmdExecutor()
    {
        return new FennelCmdExecutorImpl();
    }

    // implement FarragoSessionFactory
    public FarragoRepos newRepos(
        FarragoAllocationOwner owner,
        boolean userRepos)
    {
        return new FarragoRepos(owner, userRepos);
    }

    // implement FarragoSessionFactory
    public FennelTxnContext newFennelTxnContext(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle)
    {
        return new FennelTxnContext(repos, fennelDbHandle);
    }

    // implement FarragoSessionFactory
    public void cleanupSessions()
    {
        FarragoDatabase.shutdownConditional(0);
    }
}


// End FarragoDbSessionFactory.java
