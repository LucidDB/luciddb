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

import javax.jmi.reflect.*;

import net.sf.farrago.fem.security.*;


/**
 * FarragoSessionPrivilegeMap defines a map from object type to a set of
 * privileges relevant to that type. Map instances may be immutable, in which
 * case only read accessors may be called.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPrivilegeMap
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Registers a privilege as either legal or illegal for a type.
     *
     * @param refClass a JMI class representing the object type (e.g.
     * RelationalPackage.getCwmTable())
     * @param privilegeName name of the privilege to set; standard privilege
     * names are defined in {@link PrivilegedActionEnum}, but model extensions
     * may define their own names as well
     * @param isLegal if true, privilege is allowed on type; if false,
     * attempting to grant privilege on type will result in a validator
     * exception
     * @param includeSubclasses if true, set privilege for refClass and all of
     * its subclasses; if false, set privilege for refClass only
     */
    public void mapPrivilegeForType(
        RefClass refClass,
        String privilegeName,
        boolean isLegal,
        boolean includeSubclasses);

    /**
     * Returns a set of privileges mapped as legal for a type.
     *
     * @param refClass a JMI class representing the object type
     *
     * @return Set of privilege names
     */
    public Set<String> getLegalPrivilegesForType(RefClass refClass);
}

// End FarragoSessionPrivilegeMap.java
