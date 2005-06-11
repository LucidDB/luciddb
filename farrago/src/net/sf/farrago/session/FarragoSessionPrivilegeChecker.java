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
 * This interface specifies the privilege check service methods. The caller
 * submit request for access on each individual object along with the action to
 * be performed.
 *
 * @author Tai Tran
 * @version $Id$
 */
public interface FarragoSessionPrivilegeChecker
{
    //~ Methods ---------------------------------------------------------------

    /**
     * submit a request access to an object catalog object
     * 
     * @param obj object to be accessed
     *
     * @param authId the authorization id of the requester
     *
     * @param action the action to be performed on the object 'obj'
     *
     */
    public void requestAccess(CwmModelElement obj,  FemAuthId authId,  String action);

    /**
     * check access for all requests have been submitted 
     * 
     */
    public void checkAccess();   

}

// End FarragoSessionPrivilegeChecker.java
