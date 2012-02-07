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
package net.sf.farrago.ddl;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlGrantRoleStmt represents a DDL GRANT ROLE statement.
 *
 * @author Quoc Tai Tran
 * @version $Id$
 */
public class DdlGrantRoleStmt
    extends DdlGrantStmt
{
    //~ Instance fields --------------------------------------------------------

    protected List<SqlIdentifier> roleList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlGrantRoleStmt.
     */
    public DdlGrantRoleStmt()
    {
        super();
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoRepos repos = ddlValidator.getRepos();

        List<FemRole> grantedRoles = new ArrayList<FemRole>();
        for (SqlIdentifier roleId : roleList) {
            FemRole grantedRole =
                FarragoCatalogUtil.getRoleByName(
                    repos, roleId.getSimple());
            if (grantedRole == null) {
                throw FarragoResource.instance().ValidatorInvalidRole.ex(
                    repos.getLocalizedObjectName(roleId.getSimple()));
            }
            grantedRoles.add(grantedRole);
        }

        FemAuthId grantorAuthId = determineGrantor(ddlValidator);
        FemUser user = null;
        FemRole role = null;
        if (grantorAuthId instanceof FemUser) {
            user = (FemUser) grantorAuthId;
        } else {
            role = (FemRole) grantorAuthId;
        }
        FarragoSessionPrivilegeChecker privChecker =
            ddlValidator.getStmtValidator().getPrivilegeChecker();
        for (FemRole grantedRole : grantedRoles) {
            privChecker.requestAccess(
                grantedRole,
                user,
                role,
                PrivilegedActionEnum.INHERIT_ROLE.toString(),
                true);
        }
        privChecker.checkAccess();

        for (SqlIdentifier granteeId : granteeList) {
            // Find the repository element id for the grantee.
            FemAuthId granteeAuthId =
                FarragoCatalogUtil.getAuthIdByName(
                    repos,
                    granteeId.getSimple());
            if (granteeAuthId == null) {
                throw FarragoResource.instance().ValidatorInvalidGrantee.ex(
                    repos.getLocalizedObjectName(granteeId.getSimple()));
            }

            // For each role in the list, we instantiate a repository element
            // for the grant. Note that this makes it easier to revoke the
            // privs on an individual basis.
            for (FemRole grantedRole : grantedRoles) {
                // we could probably gang all of these up into a single
                // LURQL query, but for now execute one check per
                // granted role
                checkCycle(
                    ddlValidator.getInvokingSession(),
                    grantedRole,
                    granteeAuthId);

                // create a privilege object and set its properties
                FemGrant grant = findExistingGrant(
                    repos,
                    grantedRole,
                    grantorAuthId,
                    granteeAuthId,
                    PrivilegedActionEnum.INHERIT_ROLE.toString());
                if (grant == null) {
                    grant =
                        FarragoCatalogUtil.newRoleGrant(
                            repos,
                            grantorAuthId,
                            granteeAuthId,
                            grantedRole);
                }
                // Note that for an existing grant without admin option, we
                // upgrade in place.
                if (grantOption) {
                    grant.setWithGrantOption(true);
                }
            }
        }
    }

    private void checkCycle(
        FarragoSession session, FemAuthId grantedRole, FemAuthId granteeRole)
    {
        String lurql =
            FarragoInternalQuery.instance().SecurityRoleCycleCheck.str();
        Map<String, String> argMap = new HashMap<String, String>();
        argMap.put("grantedRoleName", grantedRole.getName());
        Collection<RefObject> result =
            session.executeLurqlQuery(
                lurql,
                argMap);
        for (RefObject o : result) {
            FemRole role = (FemRole) o;
            if (role.getName().equals(granteeRole.getName())) {
                throw FarragoResource.instance().ValidatorRoleCycle.ex(
                    session.getRepos().getLocalizedObjectName(grantedRole),
                    session.getRepos().getLocalizedObjectName(granteeRole));
            }
        }
    }

    public void setRoleList(List<SqlIdentifier> roleList)
    {
        this.roleList = roleList;
    }
}

// End DdlGrantRoleStmt.java
