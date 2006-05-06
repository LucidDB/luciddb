/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package net.sf.farrago.db;

import net.sf.farrago.fem.security.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;
import java.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import net.sf.farrago.resource.*;

/**
 * Implements the {@link FarragoSessionPrivilegeChecker} interface
 * in the context of a {@link FarragoDbSession}.
 *
 *<p>
 *
 * An instance of this class must be created per statement i.e. it can't be
 * shared between statements.
 *
 * @author Tai Tran
 * @version $Id$
 */
public class FarragoDbSessionPrivilegeChecker
    implements FarragoSessionPrivilegeChecker
{
    private final FarragoSession session;

    private final Map authMap;
    
    public FarragoDbSessionPrivilegeChecker(FarragoSession session)
    {
        this.session = session;
        authMap = new HashMap();
    }
    
    // implement FarragoSessionPrivilegeChecker
    public void requestAccess(
        CwmModelElement obj,
        FemUser user,
        FemRole role,
        String action)
    {
        List authKey = new ArrayList(2);
        authKey.add(user);
        authKey.add(role);

        // Find credentials for the given user and role.
        Set authSet = (Set) authMap.get(authKey);
        if (authSet == null) {
            // Compute all credentials for the given user and role.
            authSet = new HashSet();
            authMap.put(authKey, authSet);

            if (user != null) {
                authSet.add(user);
            }

            if (role != null) {
                authSet.add(role);
                inheritRoles(role, authSet);
            }

            authSet.add(
                FarragoCatalogUtil.getRoleByName(
                    session.getRepos(),
                    FarragoCatalogInit.PUBLIC_ROLE_NAME));
        }

        // Now, let's check their papers...
        if (testAccess(obj, authSet, action)) {
            // It's all good.
            return;
        }

        // Verboten!
        throw FarragoResource.instance().ValidatorAccessDenied.ex(
            session.getRepos().getLocalizedObjectName(action),
            session.getRepos().getLocalizedObjectName(obj));
    }
    
    // implement FarragoSessionPrivilegeChecker
    public void checkAccess()
    {
        // This implementation does all the work immediately in requestAccess,
        // so nothing to do here.
    }

    private void inheritRoles(FemRole role, Set inheritedRoles)
    {
        String inheritAction = PrivilegedActionEnum.INHERIT_ROLE.toString();
        
        Iterator grants = role.getGranteePrivilege().iterator();
        while (grants.hasNext()) {
            FemGrant grant = (FemGrant) grants.next();
            if (grant.getAction().equals(inheritAction)) {
                FemRole inheritedRole = (FemRole) grant.getElement();
                // sanity check:  DDL validation is supposed to prevent
                // cycles
                assert(!inheritedRoles.contains(inheritedRole));
                inheritedRoles.add(inheritedRole);
                inheritRoles(inheritedRole, inheritedRoles);
            }
        }
    }

    private boolean testAccess(
        CwmModelElement obj,  Set authSet, String action)
    {
        SecurityPackage sp =
            session.getRepos().getSecurityPackage();
        Iterator grants =
            sp.getPrivilegeIsGrantedOnElement().getPrivilege(obj).iterator();
        boolean sawCreationGrant = false;
        while (grants.hasNext()) {
            FemGrant grant = (FemGrant) grants.next();
            boolean isCreation =
                grant.getAction().equals(
                    PrivilegedActionEnum.CREATION.toString());
            
            if (isCreation) {
                sawCreationGrant = true;
            }
            if (authSet.contains(grant.getGrantee())
                && (grant.getAction().equals(action) || isCreation))
            {
                return true;
            }
        }
        if (sawCreationGrant) {
            return false;
        } else {
            // We didn't see a creation grant.  The only way that's possible is
            // that obj is currently in the process of being created.  In that
            // case, whatever object is referencing it must have the same
            // creator,  so no explicit privilege is required.
            return true;
        }
    }
}

// End FarragoDbSessionPrivilegeChecker.java
