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
package net.sf.farrago.db;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;


/**
 * Implements the {@link FarragoSessionPrivilegeChecker} interface in the
 * context of a {@link FarragoDbSession}.
 *
 * <p>An instance of this class must be created per statement i.e. it can't be
 * shared between statements.
 *
 * @author Tai Tran
 * @version $Id$
 */
public class FarragoDbSessionPrivilegeChecker
    implements FarragoSessionPrivilegeChecker
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoSession session;

    private final Map<List<Object>, Set<FemAuthId>> authMap;

    private FemRole publicRole;

    private final Set<String> visibleObjects;

    //~ Constructors -----------------------------------------------------------

    public FarragoDbSessionPrivilegeChecker(FarragoSession session)
    {
        this.session = session;
        authMap = new HashMap<List<Object>, Set<FemAuthId>>();
        visibleObjects = new LinkedHashSet<String>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPrivilegeChecker
    public void requestAccess(
        CwmModelElement obj,
        FemUser user,
        FemRole role,
        String action,
        boolean requireGrantOption)
    {
        List<Object> authKey = new ArrayList<Object>(3);
        authKey.add(user);
        authKey.add(role);
        authKey.add((action == null) ? "VISIBILITY" : "ACCESS");

        // Find credentials for the given user and role.
        Set<FemAuthId> authSet = authMap.get(authKey);
        if (authSet == null) {
            // Compute all credentials for the given user and role.
            authSet = new HashSet<FemAuthId>();
            authMap.put(authKey, authSet);

            if (user != null) {
                authSet.add(user);
                if (action == null) {
                    // For visibility test, include all applicable roles
                    // for user (regardless of which is active, if any)
                    authSet.addAll(FarragoCatalogUtil.getApplicableRoles(user));
                }
            }

            if (role != null) {
                authSet.add(role);
                authSet.addAll(FarragoCatalogUtil.getApplicableRoles(role));
            }

            authSet.add(getPublicRole());
        }

        // Now, let's check their papers...
        if (testAccess(obj, authSet, action, requireGrantOption)) {
            // It's all good.
            visibleObjects.add(obj.refMofId());
            return;
        }

        if (action == null) {
            // No failure, just track visibility.
            return;
        }

        // Verboten!
        if (requireGrantOption) {
            if (obj instanceof FemRole) {
                // Special-case language for failed GRANT ROLE
                assert action.equals(
                    PrivilegedActionEnum.INHERIT_ROLE.toString());
                throw FarragoResource.instance().ValidatorNoAdminOption.ex(
                    session.getRepos().getLocalizedObjectName(obj));
            } else {
                throw FarragoResource.instance().ValidatorNoGrantOption.ex(
                    session.getRepos().getLocalizedObjectName(action),
                    session.getRepos().getLocalizedObjectName(obj));
            }
        } else {
            throw FarragoResource.instance().ValidatorAccessDenied.ex(
                session.getRepos().getLocalizedObjectName(action),
                session.getRepos().getLocalizedObjectName(obj));
        }
    }

    private FemRole getPublicRole()
    {
        if (publicRole == null) {
            publicRole =
                FarragoCatalogUtil.getRoleByName(
                    session.getRepos(),
                    FarragoCatalogInit.PUBLIC_ROLE_NAME);
        }

        return publicRole;
    }

    // implement FarragoSessionPrivilegeChecker
    public void checkAccess()
    {
        // This implementation does all the work immediately in requestAccess,
        // so nothing to do here except clear the accumulated references.
        visibleObjects.clear();
    }

    // implement FarragoSessionPrivilegeChecker
    public Set<String> checkVisibility()
    {
        Set<String> set =
            new LinkedHashSet<String>(visibleObjects);
        visibleObjects.clear();
        return set;
    }

    private boolean testAccess(
        CwmModelElement obj,
        Set<FemAuthId> authSet,
        String action,
        boolean requireGrantOption)
    {
        SecurityPackage sp = session.getRepos().getSecurityPackage();
        boolean sawCreationGrant = false;
        for (Object o : sp.getPrivilegeIsGrantedOnElement().getPrivilege(obj)) {
            FemGrant grant = (FemGrant) o;
            boolean isCreation =
                grant.getAction().equals(
                    PrivilegedActionEnum.CREATION.toString());

            if (isCreation) {
                sawCreationGrant = true;
            } else {
                if (requireGrantOption && !grant.isWithGrantOption()) {
                    continue;
                }
            }

            if (authSet.contains(grant.getGrantee())
                && ((action == null)
                    || grant.getAction().equals(action) || isCreation))
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
            // creator, so no explicit privilege is required.
            return true;
        }
    }
}

// End FarragoDbSessionPrivilegeChecker.java
