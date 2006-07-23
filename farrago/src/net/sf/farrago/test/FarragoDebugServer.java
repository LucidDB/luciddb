/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import net.sf.farrago.server.*;


/**
 * FarragoDebugServer's only purpose is to provide an entry point from which
 * FarragoServer can be debugged via an IDE such as Eclipse.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDebugServer
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Provides an entry point for debugging FarragoServer.
     *
     * @param args unused
     */
    public static void main(String [] args)
        throws Exception
    {
        // Trick to invoke FarragoTestCase's static initializer to get default
        // settings for environment variables.
        FarragoQueryTest unused = new FarragoQueryTest("unused");

        FarragoVjdbcServer.main(new String[0]);
    }
}

// End FarragoDebugServer.java
