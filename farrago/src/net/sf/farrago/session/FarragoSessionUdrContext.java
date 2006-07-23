/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

    public void setSession(FarragoSession session)
    {
        this.session = session;
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
