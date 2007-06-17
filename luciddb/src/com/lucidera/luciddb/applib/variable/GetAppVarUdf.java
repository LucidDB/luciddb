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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.instance.*;

import com.lucidera.luciddb.applib.resource.*;

/**
 * SQL-invocable function to retrieve the current value of an application
 * variable.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class GetAppVarUdf
{
    public static String execute(String contextId, String varId)
    {
        if (varId == null) {
            throw ApplibResourceObject.get().AppVarIdRequired.ex();
        }
        FarragoRepos repos = null;
        try {
            repos = AppVarUtil.getRepos();
            repos.beginReposTxn(false);
            CwmExtent context = AppVarUtil.lookupContext(repos, contextId);
            CwmTaggedValue tag = AppVarUtil.lookupVariable(
                repos, context, varId);
            return tag.getValue().equals(AppVarUtil.NULL_APPVAR_VALUE)
                ? null
                : tag.getValue();
        } catch (Throwable ex) {
            throw ApplibResourceObject.get().AppVarReadFailed.ex(
                contextId, varId, ex);
        } finally {
            if (repos != null) {
                repos.endReposTxn(false);
            }
        }
    }
}

// End GetAppVarUdf.java
