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

import net.sf.farrago.fem.security.*;
import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;


/**
 * DdlSecurityHandler defines DDL handler methods for Fem Security objects of
 * type User, Role.
 *
 * @author Tai Tran
 * @version $Id$
 */
public class DdlSecurityHandler
    extends DdlHandler
{
    //~ Constructors -----------------------------------------------------------

    public DdlSecurityHandler(FarragoSessionDdlValidator validator)
    {
        super(validator);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemUser femUser)
    {
        validator.validateUniqueNames(
            repos.getCatalog(FarragoCatalogInit.SYSBOOT_CATALOG_NAME),
            repos.allOfType(FemAuthId.class),
            false);
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemRole femRole)
    {
        validator.validateUniqueNames(
            repos.getCatalog(FarragoCatalogInit.SYSBOOT_CATALOG_NAME),
            repos.allOfType(FemAuthId.class),
            false);
    }

    // implement FarragoSessionDdlHandler
    public void validateDrop(FemUser user)
    {
        // TODO: implement drop handler, including preventing the drop from
        // taking place if there are any active sessions for this user.
    }

    // implement FarragoSessionDdlHandler
    public void validateDrop(FemRole role)
    {
        // TODO: implement drop handler
    }
}

// End DdlSecurityHandler.java
