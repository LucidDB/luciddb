/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
