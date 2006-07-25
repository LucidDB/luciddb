/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.jmi;

import javax.jmi.reflect.*;

import org.netbeans.api.mdr.*;


/**
 * JmiChangeDispatcher receives change events from a {@link JmiChangeSet} and
 * dispatches them to the appropriate handler. It also supplies information
 * governing the overall behavior of the JmiChangeSet.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface JmiChangeDispatcher
{

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the MDR repository affected by the change
     */
    public MDRepository getMdrRepos();

    /**
     * @return metamodel for objects being changed
     */
    public JmiModelView getModelView();

    /**
     * @return {@link JmiDeletionAction} governing the change
     */
    public JmiDeletionAction getDeletionAction();

    /**
     * Dispatches validation of an action on an object to the correct handler.
     *
     * @param obj object affected by action
     * @param action action being validated
     */
    public void validateAction(RefObject obj, JmiValidationAction action);

    /**
     * Determines whether an object is being created. The implementation for
     * this check is model-specific.
     *
     * @param obj object to be tested
     *
     * @return true iff object is being created
     */
    public boolean isNewObject(RefObject obj);

    /**
     * TODO jvs 18-Nov-2005: explain the purpose of this or get rid of it.
     */
    public void clearDependencySuppliers(RefObject refObj);

    /**
     * Notifies this dispatcher of the effect of a deletion on a dependent
     * object.
     *
     * @param refObj dependent object
     * @param deletionAction effect
     */
    public void notifyDeleteEffect(
        RefObject refObj,
        JmiDeletionAction deletionAction);

    /**
     * Constructs a key suitable for name uniqueness testing.
     *
     * @param obj object for which to construct a key
     * @param includeType if true, object type is included in key; if false,
     * type is omitted
     *
     * @return key represented as a string, or null if object is anonymous
     */
    public String getNameKey(RefObject obj, boolean includeType);

    /**
     * Notifies this dispatcher of the detection of a collision in names
     * triggered by a call to {@link JmiChangeSet#validateUniqueNames}.
     *
     * @param container namespace in which collision occurred
     * @param newElement element being created or updated with duplicate name
     * @param oldElement existing element with same name
     * @param bothNew true if both newElement and oldElement are being created
     * simultaneously; false if oldElement is an existing persistent object
     */
    public void notifyNameCollision(
        RefObject container,
        RefObject newElement,
        RefObject oldElement,
        boolean bothNew);
}

// End JmiChangeDispatcher.java
