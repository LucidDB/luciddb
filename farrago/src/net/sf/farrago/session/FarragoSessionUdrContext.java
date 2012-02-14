/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
