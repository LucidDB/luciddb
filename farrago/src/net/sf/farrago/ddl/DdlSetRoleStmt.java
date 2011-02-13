/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
