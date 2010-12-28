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
