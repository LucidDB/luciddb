/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater.jdbc;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.session.*;

import net.sf.firewater.*;

/**
 * FirewaterLocalDriver is a JDBC driver for the Firewater SQL engine for use
 * by callers running in the same JVM with the DBMS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FirewaterLocalDriver
    extends FarragoUnregisteredJdbcEngineDriver
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        new FirewaterLocalDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // REVIEW jvs 9-Jun-2009:  this doesn't work since we package
    // the client in a separate jar.  For now we rely on the
    // net.sf.farrago.defaultSessionFactoryLibraryName property
    // setting instead.

    // implement FarragoJdbcServerDriver
    /*
    public FarragoSessionFactory newSessionFactory()
    {
        return new FirewaterSessionFactory();
    }
    */
}

// End FirewaterLocalDriver.java
