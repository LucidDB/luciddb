/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
        try {
            // Ensure that the user does not exist. We assume that the
            // repository service can enforce unique name constraint, so we
            // don't have to check that the name already exist.
            // TODO: repos should enforce unique constraints.

            // Note that no grant is created to record the user's membership in
            // PUBLIC; this is implicit during privilege check.
        } catch (Throwable ex) {
            throw res.ValidatorDefinitionInvalid.ex(
                repos.getLocalizedObjectName(femUser),
                ex);
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemRole femRole)
    {
        try {
            // WITH ADMIN grantor clause has already been dealt with during
            // parsing.  Nothing to do yet!
        } catch (Throwable ex) {
            throw res.ValidatorDefinitionInvalid.ex(
                repos.getLocalizedObjectName(femRole),
                ex);
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateModification(FemUser femUser)
    {
        // TODO: Implement the handler for Alter User...
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
