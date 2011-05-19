/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.mondrian;

import java.io.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.util.*;

import org.eigenbase.applib.resource.*;


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
    //~ Methods ----------------------------------------------------------------

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

        MondrianReplicator replicator =
            new MondrianReplicator(
                mondrianSchemaFile,
                foreignServerName,
                foreignSchemaName,
                localSchemaName,
                scriptFile,
                copyData);
        try {
            replicator.execute();
        } catch (Throwable ex) {
            throw ApplibResource.instance().MondrianReplicationFailed.ex(ex);
        } finally {
            replicator.closeAllocation();
        }
    }
}

// End ReplicateMondrianUdp.java
