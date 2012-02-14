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
package org.eigenbase.applib.variable;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.instance.*;
import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * Common definitions needed by appvar UDR's.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AppVarUtil
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String NULL_APPVAR_VALUE = "SYS$NULL";

    //~ Methods ----------------------------------------------------------------

    static FarragoRepos getRepos()
    {
        return FarragoUdrRuntime.getSession().getRepos();
    }

    static CwmExtent lookupContext(FarragoRepos repos, String contextId)
    {
        CwmExtent context =
            (CwmExtent) FarragoCatalogUtil.getModelElementByName(
                repos.getInstancePackage().getCwmExtent().refAllOfClass(),
                contextId);
        if (context == null) {
            throw ApplibResource.instance().AppVarContextUndefined.ex(
                contextId);
        }
        return context;
    }

    static CwmTaggedValue lookupVariable(
        FarragoRepos repos,
        CwmExtent context,
        String varId)
    {
        CwmTaggedValue tag =
            repos.getTag(
                context,
                varId);
        if (tag == null) {
            throw ApplibResource.instance().AppVarUndefined.ex(
                context.getName(),
                varId);
        }
        return tag;
    }
}

// End AppVarUtil.java
