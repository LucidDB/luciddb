/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.lucidera.jdbc;

import net.sf.farrago.jdbc.client.*;


/**
 * LucidDbLocalDriver is a JDBC driver for the LucidDB server for use by remote
 * clients. It is based on VJDBC.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbRmiDriver
    extends FarragoUnregisteredVjdbcClientDriver
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        new LucidDbRmiDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // override FarragoAbstractJdbcDriver
    public String getBaseUrl()
    {
        // REVIEW jvs 3-Feb-2008:  This is a hack to allow the JDBC driver
        // to be loaded off of the bootstrap class path, which is not really
        // a good thing to do.  It's half-baked because it doesn't take
        // care of other resources normally loaded from
        // FarragoRelease.properties such as default port number.  The
        // problem is that the bootstrap class loader is so "primordial"
        // that it doesn't even know how to load resources.
        // If this hack stays around, it should be applied to the other
        // LucidDB JDBC drivers as well.
        return "jdbc:luciddb:";
    }
}

// End LucidDbRmiDriver.java
