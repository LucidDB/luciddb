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

package net.sf.farrago.db;

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
 * Implements the {@link FarragoSessionPrivilegeChecker} interface
 * in the context of a {@link FarragoDbSession}.
 *
 *<p>
 *
 * An instance of this class must be created per statement i.e. it can't be
 * shared between statements.
 *
 *<p>
 *
 * REVIEW jvs 10-June-2005:  I don't think we need (a) below.  It's
 * fine to state the expected usage, but we're defining a general-purpose
 * API, so there's no need to constrain the caller in this way.
 *
 *<p>
 *
 * TODO:
 * (a) Check that all requests come from the same statement handle.
 * (b) Do cache of underlying graph. It can be slow to keep reading.
 *
 * @author Tai Tran
 * @version $Id$
 */
class FarragoDbSessionPrivilegeChecker implements FarragoSessionPrivilegeChecker
{
    private DoubleKeyMap requestsMap;

    // constructor
    public FarragoDbSessionPrivilegeChecker(FarragoSession session)
    {
        requestsMap = new DoubleKeyMap();
    }
    
    // implement FarragoSessionPrivilegeChecker
    public void requestAccess(CwmModelElement obj,  FemAuthId authId,
        String action)
    {
        // TODO: Should consider a way of checking that the request is coming
        // from the same statement. E.g. should a statement handle be kept a
        // check each time.
        
        // add the request to the queue
        requestsMap.put(obj, authId,  action);
    }

    
    // implement FarragoSessionPrivilegeChecker
    public void checkAccess()
    {
        // TODO: for efficieny purpose, all objects and their corresponding
        // accessor must be provided as one single LURQL query

        
        // TODO: Use LURQL to query all privileges for obj, authid and compare
        // that with the requested actions respectively
    }
}

// End FarragoDbSessionPrivilegeChecker.java
