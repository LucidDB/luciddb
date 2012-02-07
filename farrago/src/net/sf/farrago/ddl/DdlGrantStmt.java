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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * DdlGrantStmt represents a DDL GRANT statement.
 *
 * @author Quoc Tai Tran
 * @version $Id$
 */
public abstract class DdlGrantStmt
    extends DdlStmt
{
    public enum GrantorReference
    {
        OMITTED, CURRENT_USER, CURRENT_ROLE
    }

    //~ Instance fields --------------------------------------------------------

    protected boolean grantOption;
    protected boolean currentRoleOption;
    protected boolean currentUserOption;
    protected List<SqlIdentifier> granteeList;
    protected GrantorReference grantorReference;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlGrantStmt.
     */
    public DdlGrantStmt()
    {
        super(null);
        grantorReference = GrantorReference.OMITTED;
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    public void setGranteeList(List<SqlIdentifier> granteeList)
    {
        this.granteeList = granteeList;
    }

    public void setGrantOption(boolean grantOption)
    {
        this.grantOption = grantOption;
    }

    public void setGrantorReference(GrantorReference grantorReference)
    {
        this.grantorReference = grantorReference;
    }

    protected FemAuthId determineGrantor(
        FarragoSessionDdlValidator ddlValidator)
    {
        FemAuthId grantorAuthId = null;

        String currentUser =
            ddlValidator.getInvokingSession().getSessionVariables()
            .currentUserName;
        String currentRole =
            ddlValidator.getInvokingSession().getSessionVariables()
            .currentRoleName;
        String grantorName = null;

        switch (grantorReference) {
        case OMITTED:
            if (JmiObjUtil.isBlank(currentUser)) {
                grantorName = currentRole;
            } else {
                grantorName = currentUser;
            }
            break;
        case CURRENT_USER:
            grantorName = currentUser;
            break;
        case CURRENT_ROLE:
            grantorName = currentRole;
            break;
        }
        if (!JmiObjUtil.isBlank(grantorName)) {
            grantorAuthId =
                FarragoCatalogUtil.getAuthIdByName(
                    ddlValidator.getRepos(),
                    grantorName);
        }
        if (grantorAuthId == null) {
            throw FarragoResource.instance().ValidatorInvalidGrantor.ex();
        }

        return grantorAuthId;
    }

    protected FemGrant findExistingGrant(
        FarragoRepos repos,
        CwmModelElement obj,
        FemAuthId grantor,
        FemAuthId grantee,
        String action)
    {
        SecurityPackage sp = repos.getSecurityPackage();
        for (FemGrant grant
                 : sp.getPrivilegeIsGrantedOnElement().getPrivilege(obj))
        {
            if (!grant.getAction().equals(action)) {
                continue;
            }
            if (!grant.getGrantee().equals(grantee)) {
                continue;
            }
            if (!grant.getGrantor().equals(grantor)) {
                continue;
            }
            return grant;
        }
        return null;
    }
}

// End DdlGrantStmt.java
