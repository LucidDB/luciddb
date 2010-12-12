/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

    public FemAuthId determineGrantor(FarragoSessionDdlValidator ddlValidator)
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
}

// End DdlGrantStmt.java
