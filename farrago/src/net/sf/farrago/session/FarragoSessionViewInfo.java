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
package net.sf.farrago.session;

import java.sql.*;
import java.util.*;


/**
 * FarragoSessionViewInfo defines internal information needed while creating a
 * view.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionViewInfo
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The query definition expanded after validation.  This contains no
     * context-dependent information (e.g. all objects are fully qualified),
     * so it can be stored in the catalog.
     */
    public String validatedSql;

    /**
     * Set of CwmNamedColumnSet instances on which this view directly depends
     * (i.e. other views are not expanded).
     */
    public Set dependencies;

    /**
     * Metadata for result set returned when this view is queried.
     */
    public ResultSetMetaData resultMetaData;

    /**
     * Metadata for parameters used as input to this view.
     */
    public ParameterMetaData parameterMetaData;
}


// End FarragoSessionViewInfo.java
