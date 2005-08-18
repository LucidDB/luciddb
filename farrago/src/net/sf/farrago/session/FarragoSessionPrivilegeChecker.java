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
package net.sf.farrago.session;

import net.sf.farrago.fem.security.*;
import net.sf.farrago.cwm.core.*;

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
    //~ Methods ---------------------------------------------------------------

    /**
     * Submits a request for access to a catalog object.  Actual
     * checking of the request may be deferred until the next
     * call to checkAccess.
     * 
     * @param obj object to be accessed
     *
     * @param authId the authorization id of the requester
     *
     * @param action the action to be performed on obj
     */
    public void requestAccess(
        CwmModelElement obj,
        FemAuthId authId,
        String action);

    /**
     * Checks access for all requests that have been submitted,
     * and clears the request list.
     */
    public void checkAccess();   

}

// End FarragoSessionPrivilegeChecker.java
