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

import net.sf.farrago.ddl.*;


/**
 * FarragoSessionModelExtension defines the SPI for plugging in custom behavior
 * for model extensions. Although model extensions may be independent of session
 * personalities, they still interact with them, which is why they are defined
 * at this level.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionModelExtension
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Defines the handlers to be used to validate and execute DDL actions for
     * object types in this model extension. See {@link
     * FarragoSessionDdlHandler} for an explanation of how to define the handler
     * objects in this list. Optionally, may also define drop rules.
     *
     * @param ddlValidator validator which will invoke handlers
     * @param handlerList receives handler objects in order in which they should
     */
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List<DdlHandler> handlerList);

    /**
     * Defines resource bundles to be used for localizing model object names.
     * Any resource having a name of the form "UmlXXX" will be interpreted as
     * the localized name for UML class XXX.
     *
     * @param bundleList receives instances of ResourceBundle
     */
    public void defineResourceBundles(List<ResourceBundle> bundleList);

    /**
     * Defines privileges allowed on various object types.
     *
     * @param map receives allowed privileges
     */
    public void definePrivileges(
        FarragoSessionPrivilegeMap map);
}

// End FarragoSessionModelExtension.java
