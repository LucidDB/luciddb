/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.jdbc.engine;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.jdbc.*;

import java.sql.*;
import java.util.*;

/**
 * FarragoJdbcRoutineDriver implements the JDBC driver used for
 * default connections from user-defined routines.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcRoutineDriver
    extends FarragoAbstractJdbcDriver
    implements Driver
{
    // NOTE jvs 19-Jan-2005:  let FarragoJdbcEngineDriver register us,
    // since no one else should be referencing us directly

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcRoutineDriver object.
     */
    public FarragoJdbcRoutineDriver()
    {
    }

    // implement FarragoAbstractJdbcDriver
    public String getBaseUrl()
    {
        return "jdbc:default:connection";
    }
    
    // implement FarragoAbstractJdbcDriver
    public String getUrlPrefix()
    {
        return getBaseUrl();
    }
    
    // implement Driver
    public Connection connect(
        String url,
        Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            if (!url.equals(getBaseUrl())) {
                throw FarragoResource.instance().newJdbcInvalidUrl(url);
            }
            return FarragoRuntimeContext.newConnection();
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }
}

// End FarragoJdbcRoutineDriver.java
