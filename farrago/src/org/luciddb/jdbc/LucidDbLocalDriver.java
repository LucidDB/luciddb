/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.luciddb.jdbc;

import org.luciddb.session.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.session.*;


/**
 * LucidDbLocalDriver is a JDBC driver for the LucidDB engine for use by callers
 * running in the same JVM with the DBMS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbLocalDriver
    extends FarragoUnregisteredJdbcEngineDriver
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        new LucidDbLocalDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoJdbcServerDriver
    public FarragoSessionFactory newSessionFactory()
    {
        return new LucidDbSessionFactory();
    }
}

// End LucidDbLocalDriver.java
