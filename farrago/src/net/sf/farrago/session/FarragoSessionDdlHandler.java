/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import net.sf.farrago.cwm.core.*;

/**
 * FarragoSessionDdlHandler is for illustration purposes only; it is not meant
 * to be implemented.  The methods declared here are templates for the handler
 * methods invoked by FarragoSessionDdlValidator for each object affected by a
 * DDL statement.
 *
 *<p>
 *
 * Real handler implementations should provide overloads of these
 * methods for all of the specific subtypes of CwmModelElement
 * which they wish to handle.  These overloads will be invoked
 * reflectively via {@link org.eigenbase.util.ReflectUtil#invokeVisitor}.
 *
 *<p>
 *
 * If no handler is present for a particular type/action combination,
 * FarragoSessionDdlValidator will assume that no special handling is required.
 * Invocation uses the
 * {@link org.eigenbase.util.Glossary#ChainOfResponsibilityPattern},
 * invoking handlers in order until one returns true.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionDdlHandler
{
    /**
     * Validates the definition of an object when it is created.
     *
     * @param modelElement element being validated
     */
    public void validateDefinition(CwmModelElement modelElement);

    /**
     * Validates the new definition of an object when it is altered.
     *
     * @param modelElement element being altered
     */
    public void validateModification(CwmModelElement modelElement);
    
    /**
     * Validates that an object can be dropped.
     *
     * @param modelElement element being dropped
     */
    public void validateDrop(CwmModelElement modelElement);

    /**
     * Validates that an object can be truncated.
     *
     * @param modelElement element being validated
     */
    public void validateTruncation(CwmModelElement modelElement);

    /**
     * Executes creation of an object; for example, if modelElement
     * has stored data associated with it, this call should
     * allocate and initialize initial storage for the object.
     *
     * @param modelElement element being created
     */
    public void executeCreation(CwmModelElement modelElement);

    /**
     * Executes modification of an object.
     *
     * @param modelElement element being modified
     */
    public void executeModification(CwmModelElement modelElement);

    /**
     * Executes drop of an object; for example, if modelElement
     * has stored data associated with it, this call should
     * deallocate the object's storage.
     *
     * @param modelElement element being created
     */
    public void executeDrop(CwmModelElement modelElement);
    
    /**
     * Executes truncation of an object; for example, if modelElement
     * has stored data associated with it, this call should
     * deallocate and reinitialize the object's storage.
     *
     * @param modelElement element being created
     */
    public void executeTruncation(CwmModelElement modelElement);
}

// End FarragoSessionDdlHandler.java
