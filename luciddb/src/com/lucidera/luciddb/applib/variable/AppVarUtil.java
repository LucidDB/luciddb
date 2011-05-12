/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.variable;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.instance.*;

import com.lucidera.luciddb.applib.resource.*;

/**
 * Common definitions needed by appvar UDR's.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AppVarUtil
{
    public static final String NULL_APPVAR_VALUE = "SYS$NULL";

    static FarragoRepos getRepos()
    {
        return FarragoUdrRuntime.getSession().getRepos();
    }

    static CwmExtent lookupContext(FarragoRepos repos, String contextId)
    {
        CwmExtent context = (CwmExtent)
            FarragoCatalogUtil.getModelElementByName(
                repos.getInstancePackage().getCwmExtent().refAllOfClass(),
                contextId);
        if (context == null) {
            throw ApplibResourceObject.get().AppVarContextUndefined.ex(
                contextId);
        }
        return context;
    }

    static CwmTaggedValue lookupVariable(
        FarragoRepos repos, CwmExtent context, String varId)
    {
        CwmTaggedValue tag = repos.getTag(
            context,
            varId);
        if (tag == null) {
            throw ApplibResourceObject.get().AppVarUndefined.ex(
                context.getName(), varId);
        }
        return tag;
    }
}

// End AppVarUtil.java
