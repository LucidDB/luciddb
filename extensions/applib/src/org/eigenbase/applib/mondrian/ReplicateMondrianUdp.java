/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
