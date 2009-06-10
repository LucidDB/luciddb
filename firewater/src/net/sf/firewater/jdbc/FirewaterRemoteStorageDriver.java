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

import com.lucidera.farrago.*;

import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.session.*;

/**
 * FirewaterRemoteStorageDriver is a JDBC driver used by a Firewater
 * engine to connect to remote storage via RMI.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterRemoteStorageDriver
    extends FarragoUnregisteredVjdbcClientDriver
{
    static {
        new FirewaterRemoteStorageDriver().register();
    }

    // override FarragoAbstractJdbcDriver
    public String getBaseUrl()
    {
        return "jdbc:firewater_storage:remote:";
    }
}

// End FirewaterRemoteStorageDriver.java
