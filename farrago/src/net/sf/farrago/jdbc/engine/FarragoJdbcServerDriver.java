/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import net.sf.farrago.session.*;


/**
 * FarragoJdbcServerDriver defines the interface which must be implemented
 * by JDBC drivers which can be used to implement {@link FarragoServer}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoJdbcServerDriver extends Driver
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a new FarragoSessionFactory which will govern the behavior
     * of connections established through this driver.
     *
     * @return new factory
     */
    public FarragoSessionFactory newSessionFactory();

    /**
     * @return the base JDBC URL for this driver;
     * subclassing drivers can override this to customize the URL scheme
     */
    public String getBaseUrl();
}


// End FarragoJdbcServerDriver.java
