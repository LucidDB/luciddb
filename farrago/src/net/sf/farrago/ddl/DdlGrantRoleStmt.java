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
package net.sf.farrago.ddl;

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
 * DdlGrantRoleStmt represents a DDL GRANT ROLE statement.
 * 
 *
 * @author Quoc Tai Tran
 * @version $Id$
 */
public class DdlGrantRoleStmt extends DdlGrantStmt
{
    protected List roleList;
    

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructs a new DdlGrantRoleStmt.
     *
     */
    public DdlGrantRoleStmt()
    {
        super();
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoRepos repos =  ddlValidator.getRepos();

        FemAuthId grantorAuthId = determineGrantor(ddlValidator);

        // TODO: Check that for all roles to be granted 
        // (a) the grantor must be the owner. Or
        // (b) the owner has been granted with Admin Option. Need model change!
        
        Iterator iter = granteeList.iterator();
        while(iter.hasNext()) {

            // process the next grantee
            SqlIdentifier granteeId = (SqlIdentifier) iter.next();

            // Find the repository element id for the grantee,  create one if
            // it does not exist
            FemAuthId granteeAuthId = FarragoCatalogUtil.getAuthIdByName(repos, granteeId.getSimple());

            // for each role in the list, we instantiate a repository
            // element. Note that this makes it easier to revoke the privs on
            // the individual basis.
            Iterator iterRole = roleList.iterator();
            while (iterRole.hasNext()) {
                
                SqlIdentifier roleId = (SqlIdentifier) iterRole.next();
                
                
                // create a privilege object and set its properties
                FemGrant grant = FarragoCatalogUtil.newRoleGrant(
                    repos, grantorAuthId.getName(), granteeId.getSimple(), roleId.getSimple());                
                
                // set the privilege name (i.e. action) and properties
                grant.setWithGrantOption(grantOption);
            }
        }
    }    

    public void setRoleList(List roleList)
    {
        this.roleList = roleList;
    }    
}

// End DdlGrantRoleStmt.java
