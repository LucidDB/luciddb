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
package net.sf.farrago.session;

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.security.*;

/**
 * This interface specifies the privilege check service methods. The caller
 * submits request for access on each individual object along with the action to
 * be performed.
 *
 * @author Tai Tran
 * @version $Id$
 */
public interface FarragoSessionPrivilegeChecker
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Submits a request for access to a catalog object. Actual checking of the
     * request may be deferred until the next call to checkAccess or
     * checkVisibility. It is legal to specify neither, one, or both of user
     * and role; even when both are null, privileges granted to PUBLIC still
     * apply.
     *
     * @param obj object to be accessed
     * @param user the requesting user, or null for none
     * @param role the requesting role, or null for none
     * @param action the action to be performed on obj (see {@link
     * PrivilegedActionEnum} for base set), or null for a visibility
     * check (any privilege is good enough)
     * @param requireGrantOption whether the privilege needs to
     * have been granted WITH GRANT OPTION
     */
    public void requestAccess(
        CwmModelElement obj,
        FemUser user,
        FemRole role,
        String action,
        boolean requireGrantOption);

    /**
     * Checks access for all requests that have been submitted (throwing
     * an exception if any fail), and clears the
     * request list if all pass.
     */
    public void checkAccess();

    /**
     * Checks object visibility for all requests that have been submitted,
     * and clears the request list.
     *
     * @return set of MOFID's for visible objects
     */
    public Set<String> checkVisibility();
}

// End FarragoSessionPrivilegeChecker.java
