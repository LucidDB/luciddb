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
 * Common definitions needed by appvar UDR's.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AppVarUtil
{
    public static final String CURRENT_VALUE_KEY = "currentValue";
    
    public static final String DESCRIPTION_KEY = "description";

    static Preferences getPreferencesNode(
        String contextId, String varId, boolean create)
        throws BackingStoreException
    {
        Preferences root = Preferences.userNodeForPackage(AppVarUtil.class);

        // In developer/test environments, use EIGEN_HOME to
        // discriminate by branch.
        String eigenHome = System.getenv("EIGEN_HOME");
        if (eigenHome != null) {
            eigenHome = eigenHome.replace('/', '!');
            root = root.node(eigenHome);
        }
        
        if (!create && !root.nodeExists(contextId)) {
            throw ApplibResourceObject.get().AppVarContextUndefined.ex(
                contextId);
        }
        Preferences contextNode = root.node(contextId);
        if (varId == null) {
            return contextNode;
        }
        if (!create && !contextNode.nodeExists(varId)) {
            throw ApplibResourceObject.get().AppVarUndefined.ex(
                contextId, varId);
        }
        return contextNode.node(varId);
    }
}

// End AppVarUtil.java
