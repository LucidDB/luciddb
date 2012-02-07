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
package net.sf.farrago.fennel;

import java.sql.*;

import net.sf.farrago.fem.fennel.*;


/**
 * FennelCmdExecutor defines a mechanism for extending and modifying the command
 * set understood by Fennel. {@link FennelCmdExecutorImpl} provides a default
 * implementation. Extensions can be created by writing a JNI DLL which links
 * with Farrago's JNI DLL and provides an alternative for {@link
 * FennelStorage#executeJavaCmd}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelCmdExecutor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Executes one FemCmd with an optional execution handle.
     *
     * @param cmd the command to be executed
     * @param execHandle the execution handle used to communicate state
     * information from Farrago to Fennel; set to null if there is no handle
     *
     * @return result handle as primitive
     */
    public long executeJavaCmd(FemCmd cmd, FennelExecutionHandle execHandle)
        throws SQLException;
}

// End FennelCmdExecutor.java
