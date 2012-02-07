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
