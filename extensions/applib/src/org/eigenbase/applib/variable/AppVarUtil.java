/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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
