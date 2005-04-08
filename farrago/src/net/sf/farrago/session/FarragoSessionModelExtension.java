/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

/**
 * FarragoSessionModelExtension defines the SPI for plugging in custom
 * behavior for model extensions.  Although model extensions may
 * be independent of session personalities, they still interact with
 * them, which is why they are defined at this level.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionModelExtension
{
    /**
     * Defines the handlers to be used to validate and execute DDL actions for
     * object types in this model extension.  See {@link
     * FarragoSessionDdlHandler} for an explanation of how to define the
     * handler objects in this list.  Optionally, may also define drop rules.
     *
     * @param ddlValidator validator which will invoke handlers
     *
     * @param handlerList receives handler objects in order in which they should
     * be tried
     */
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List handlerList);
}

// End FarragoSessionModelExtension.java
