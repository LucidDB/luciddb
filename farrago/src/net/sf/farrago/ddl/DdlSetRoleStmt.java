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
import net.sf.farrago.fem.security.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * DdlSetRoleStmt represents the DDL statement SET ROLE.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetRoleStmt
    extends DdlSetContextStmt
{
    //~ Instance fields --------------------------------------------------------

    private SqlIdentifier roleName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetRoleStmt.
     *
     * @param valueExpr value expression for new role
     */
    public DdlSetRoleStmt(SqlNode valueExpr)
    {
        super(valueExpr);
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        if (getValueExpression() == null) {
            // This came from SET ROLE NONE
            roleName = new SqlIdentifier("", SqlParserPos.ZERO);
            return;
        }
        super.preValidate(ddlValidator);
        if (parsedExpr instanceof SqlIdentifier) {
            roleName = (SqlIdentifier) parsedExpr;
            if (!roleName.isSimple()) {
                roleName = null;
            }
        }
        FemRole targetRole = null;
        if (roleName != null) {
            targetRole = FarragoCatalogUtil.getRoleByName(
                ddlValidator.getRepos(),
                roleName.getSimple());
            if (targetRole == null) {
                roleName = null;
            }
        }
        if (roleName != null) {
            FarragoSessionVariables sessionVariables =
                ddlValidator.getInvokingSession().getSessionVariables();
            FemUser currentUser = FarragoCatalogUtil.getUserByName(
                ddlValidator.getRepos(),
                sessionVariables.currentUserName);
            Set<FemRole> applicableRoles =
                FarragoCatalogUtil.getApplicableRoles(currentUser);
            if (!applicableRoles.contains(targetRole)) {
                roleName = null;
            }
        }
        if (roleName == null) {
            throw FarragoResource.instance().ValidatorSetRoleInvalidExpr.ex(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public SqlIdentifier getRoleName()
    {
        return roleName;
    }
}

// End DdlSetRoleStmt.java
