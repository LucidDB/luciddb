/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

import java.util.prefs.*;

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
        throws BackingStoreException
    {
        if (varId == null) {
            throw ApplibResourceObject.get().AppVarIdRequired.ex();
        }
        try {
            Preferences prefs = AppVarUtil.getPreferencesNode(
                contextId, varId, false);
            return prefs.get(AppVarUtil.CURRENT_VALUE_KEY, null);
        } catch (BackingStoreException ex) {
            throw ApplibResourceObject.get().AppVarReadFailed.ex(
                contextId, varId, ex);
        }
    }
}

// End GetAppVarUdf.java
