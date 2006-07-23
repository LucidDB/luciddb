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
     * @return Set<String> where each String is a privilege name
     */
    public Set getLegalPrivilegesForType(RefClass refClass);
}

// End FarragoSessionPrivilegeMap.java
