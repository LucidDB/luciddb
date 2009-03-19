/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;


/**
 * This class provides internal support for the implementation of {@link
 * net.sf.farrago.runtime.FarragoUdrRuntime}. One instance is allocated to
 * correspond to each code-generated method invocation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionUdrContext
{
    //~ Instance fields --------------------------------------------------------

    private final String invocationId;
    private final String serverMofId;

    private FarragoSession session;
    private FarragoRepos repos; // allows for overridding of the repository

    // that UDX's use.  Useful in a distributed extension of Farrago where
    // UDX might run in a variety of environments (especially for environments
    // without a FarragoSession)
    private Object obj;

    //~ Constructors -----------------------------------------------------------

    public FarragoSessionUdrContext(
        String invocationId,
        String serverMofId)
    {
        this.invocationId = invocationId;
        this.serverMofId = serverMofId;
        this.obj = null;
        this.session = null;
        this.repos = null;
    }

    //~ Methods ----------------------------------------------------------------

    public String getInvocationId()
    {
        return invocationId;
    }

    public String getServerMofId()
    {
        return serverMofId;
    }

    public FarragoSession getSession()
    {
        return session;
    }

    public FarragoRepos getRepos()
    {
        if (repos != null) {
            return repos;
        }
        return session.getRepos();
    }

    public void setSession(FarragoSession session)
    {
        this.session = session;
    }

    public void setRepos(FarragoRepos repos)
    {
        this.repos = repos;
    }

    public Object getObject()
    {
        return obj;
    }

    public void setObject(Object obj)
    {
        this.obj = obj;
    }
}

// End FarragoSessionUdrContext.java
