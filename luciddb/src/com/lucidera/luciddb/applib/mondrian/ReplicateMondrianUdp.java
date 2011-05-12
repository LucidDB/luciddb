/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.mondrian;

import java.util.*;
import java.io.*;
import java.sql.*;

import net.sf.farrago.util.*;

import com.lucidera.luciddb.applib.resource.*;

/**
 * Implements the <a
 * href="http://docs.eigenbase.org/LucidDbAppLib_REPLICATE_MONDRIAN">
 * APPLIB.REPLICATE_MONDRIAN</a> UDP.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class ReplicateMondrianUdp
{
    public static void execute(
        String mondrianSchemaFileName,
        String foreignServerName,
        String foreignSchemaName,
        String localSchemaName,
        String scriptFileName,
        boolean copyData)
        throws Exception
    {
        mondrianSchemaFileName =
            FarragoProperties.instance().expandProperties(
                mondrianSchemaFileName);
        File mondrianSchemaFile = new File(mondrianSchemaFileName);
        
        scriptFileName =
            FarragoProperties.instance().expandProperties(
                scriptFileName);
        File scriptFile = new File(scriptFileName);

        MondrianReplicator replicator = new MondrianReplicator(
            mondrianSchemaFile,
            foreignServerName,
            foreignSchemaName,
            localSchemaName,
            scriptFile,
            copyData);
        try {
            replicator.execute();
        } catch (Throwable ex) {
            throw ApplibResourceObject.get().MondrianReplicationFailed.ex(ex);
        } finally {
            replicator.closeAllocation();
        }
    }
}

// End ReplicateMondrianUdp.java
