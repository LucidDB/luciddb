/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

        FemAuthId grantorAuthId = determineGrantor(ddlValidator);

        // TODO: Check that for all roles to be granted  (a) the grantor must be
        // the owner. Or (b) the owner has been granted with Admin Option. Need
        // model change!

        for (SqlIdentifier granteeId : granteeList) {
            // REVIEW: SWZ: 2008-07-29: getAuthIdByName most certainly does not
            // create an AuthId if it does not exist.  An optimization here
            // would be to modify newRoleGrant to accept AuthId instances
            // instead of re-doing the lookup for granteeId for each role in the
            // roleList.

            // Find the repository element id for the grantee,  create one if
            // it does not exist
            FemAuthId granteeAuthId =
                FarragoCatalogUtil.getAuthIdByName(
                    repos,
                    granteeId.getSimple());

            // for each role in the list, we instantiate a repository
            // element. Note that this makes it easier to revoke the privs on
            // the individual basis.
            for (SqlIdentifier roleId : roleList) {
                // create a privilege object and set its properties
                FemGrant grant =
                    FarragoCatalogUtil.newRoleGrant(
                        repos,
                        grantorAuthId.getName(),
                        granteeId.getSimple(),
                        roleId.getSimple());

                // set the privilege name (i.e. action) and properties
                grant.setWithGrantOption(grantOption);
            }
        }
    }

    public void setRoleList(List<SqlIdentifier> roleList)
    {
        this.roleList = roleList;
    }
}

// End DdlGrantRoleStmt.java
