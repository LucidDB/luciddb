/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.catalog;

import net.sf.farrago.cwm.relational.*;

/**
 * FarragoConnectionDefaults defines default values in effect for a connection
 * to Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoConnectionDefaults implements Cloneable
{
    /**
     * The name of the default CwmCatalog qualifier, changed by SET CATALOG.
     * Can never be null.
     */
    public String catalogName;

    /**
     * The name of the catalog for the default CwmSchema qualifier, changed by
     * SET SCHEMA.  Can be null to indicate no default schema has been set yet.
     */
    public String schemaCatalogName;
    
    /**
     * The name of the default CwmSchema qualifier, changed by SET SCHEMA.  Can
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

    public FarragoConnectionDefaults cloneDefaults()
    {
        try {
            return (FarragoConnectionDefaults) clone();
        } catch (CloneNotSupportedException ex) {
            throw new AssertionError();
        }
    }
}

// End FarragoConnectionDefaults.java
