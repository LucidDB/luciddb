/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
 * FarragoServerTest tests Farrago client/server connections via VJDBC.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoVjdbcServerTest
    extends FarragoServerTest
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new FarragoVjdbcServerTest.
     *
     * @param testCaseName JUnit test case name
     */
    public FarragoVjdbcServerTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoAbstractServer newServer()
    {
        return new FarragoVjdbcServer();
    }

    protected FarragoAbstractJdbcDriver newClientDriver()
    {
        return new FarragoVjdbcClientDriver();
    }


    public void testExceptionContents() throws Throwable
    {
        // JDF 02/06/08 This override is due to a limititation imposed by Vjdbc.
        // FarragoSqlException inherits SQLException.  Vjdbc will only pass
        // generic SQLExceptions.  If it is an exception that extends SQLException a new
        // SQLException is allocated and populated from the original exception.
        // See vjdbc.util.SqlExceptionHelper.wrap().  Bascially this is a bad method 
        // name since either it returns the original exception or creates a whole
        // new one and discards the old.
    }
}

// End FarragoVjdbcServerTest.java
