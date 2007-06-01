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
    //~ Instance fields --------------------------------------------------------

    protected boolean grantOption;
    protected boolean currentRoleOption;
    protected boolean currentUserOption;
    protected List<SqlIdentifier> granteeList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlGrantStmt.
     */
    public DdlGrantStmt()
    {
        super(null);
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

    public void setCurrentRoleOption(boolean currentRoleOption)
    {
        this.currentRoleOption = currentRoleOption;
    }

    public void setCurrentUserOption(boolean currentUserOption)
    {
        this.currentUserOption = currentUserOption;
    }

    public FemAuthId determineGrantor(FarragoSessionDdlValidator ddlValidator)
    {
        FemAuthId grantorAuthId;

        if (currentRoleOption == true) {
            // TODO: retrieve the current role from the session and set that to
            // be the grantor
            grantorAuthId = null;
        } else {
            // Either
            // (a) CURRENT_USER is specified in the GRANTED BY clause or
            // (b) the GRANTED BY clause is missing,
            // then we use current session user as the grantor.

            String grantorName =
                ddlValidator.getInvokingSession().getSessionVariables()
                .currentUserName;
            grantorAuthId =
                FarragoCatalogUtil.getAuthIdByName(
                    ddlValidator.getRepos(),
                    grantorName);
        }
        assert (grantorAuthId != null);

        return grantorAuthId;
    }
}

// End DdlGrantStmt.java
