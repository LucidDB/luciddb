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

import net.sf.farrago.cwm.core.*;


/**
 * FarragoSessionDdlHandler is for illustration purposes only; it is not meant
 * to be implemented. The methods declared here are templates for the handler
 * methods invoked by FarragoSessionDdlValidator for each object affected by a
 * DDL statement.
 *
 * <p>Real handler implementations should provide overloads of these methods for
 * all of the specific subtypes of CwmModelElement which they wish to handle.
 * These overloads will be invoked reflectively via {@link
 * org.eigenbase.util.ReflectUtil#invokeVisitor}.
 *
 * <p>If no handler is present for a particular type/action combination,
 * FarragoSessionDdlValidator will assume that no special handling is required.
 * Invocation uses the {@link
 * org.eigenbase.util.Glossary#ChainOfResponsibilityPattern}, invoking handlers
 * in order until one returns true.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionDdlHandler
{
    //~ Methods ----------------------------------------------------------------

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
     * Executes creation of an object; for example, if modelElement has stored
     * data associated with it, this call should allocate and initialize initial
     * storage for the object.
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
     * Executes drop of an object; for example, if modelElement has stored data
     * associated with it, this call should deallocate the object's storage.
     *
     * @param modelElement element being created
     */
    public void executeDrop(CwmModelElement modelElement);

    /**
     * Executes truncation of an object; for example, if modelElement has stored
     * data associated with it, this call should deallocate and reinitialize the
     * object's storage.
     *
     * @param modelElement element being created
     */
    public void executeTruncation(CwmModelElement modelElement);
}

// End FarragoSessionDdlHandler.java
