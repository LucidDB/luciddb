/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import net.sf.farrago.cwm.relational.*;

import java.util.*;

/**
 * FarragoSessionVariables defines global variable settings for a Farrago
 * session.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionVariables implements Cloneable
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The name of the default catalog qualifier, changed by SET CATALOG.
     * Can never be null.
     */
    public String catalogName;

    /**
     * The name of the default schema qualifier, changed by SET SCHEMA.  Can
     * be null to indicate no default schema has been set yet.
     */
    public String schemaName;

    /**
     * Value of SQL expression SYSTEM_USER.
     */
    public String systemUserName;

    /**
     * Value of SQL expression SESSION_USER.
     */
    public String sessionUserName;

    /**
     * Value of SQL expression CURRENT_USER.
     */
    public String currentUserName;

    /**
     * Value of SQL expression CURRENT_PATH as a list of schemas.  Entries are
     * SqlIdentifiers (catalog.schema).  This list is immutable to prevent
     * accidental aliasing.
     */
    public List schemaSearchPath;

    //~ Methods ---------------------------------------------------------------

    public FarragoSessionVariables cloneVariables()
    {
        try {
            return (FarragoSessionVariables) clone();
        } catch (CloneNotSupportedException ex) {
            throw new AssertionError();
        }
    }
}


// End FarragoSessionVariables.java
