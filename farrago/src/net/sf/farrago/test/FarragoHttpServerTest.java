/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
package net.sf.farrago.test;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.server.*;

/**
 * FarragoServerTest tests Farrago client/server connections via VJDBC's
 * HTTP implementation.
 *
 *<p>
 *
 * TODO jvs 30-Sept-2009:  validate and re-enable tests with JRockit since
 * HTTP should be OK there (no RMI distributed gc to cause bumps).
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoHttpServerTest extends FarragoVjdbcServerTest
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new FarragoHttpServerTest.
     *
     * @param testCaseName JUnit test case name
     */
    public FarragoHttpServerTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoAbstractServer newServer()
    {
        FarragoVjdbcServer server = new FarragoVjdbcServer();
        // default protocol is HTTP
        return server;
    }

    protected FarragoAbstractJdbcDriver newClientDriver()
    {
        return new FarragoVjdbcHttpClientDriver();
    }
}

// End FarragoHttpServerTest.java
