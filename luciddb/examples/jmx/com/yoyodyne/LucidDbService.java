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
package com.yoyodyne;

import org.jboss.system.*;
import org.jboss.logging.util.*;
import org.apache.log4j.*;

import com.lucidera.jdbc.*;
import com.lucidera.farrago.*;

public class LucidDbService
    extends ServiceMBeanSupport
    implements LucidDbServiceMBean
{
    private LucidDbServer dbmsServer;
    
    public void startService() throws Exception
    {
        // Create a driver for loading LucidDB within the same JVM.
        LucidDbLocalDriver jdbcDriver = new LucidDbLocalDriver();

        // Start the LucidDB server
        dbmsServer = new LucidDbServer(
            new LoggerWriter(Logger.getLogger("LucidDB")));
        dbmsServer.start(jdbcDriver);
    }
    
    public void stopService() throws Exception
    {
        if (dbmsServer != null) {
            if (!dbmsServer.stopSoft()) {
                dbmsServer.getPrintWriter().println("Killing all sessions...");
                dbmsServer.stopHard();
            }
        }
        dbmsServer = null;
    }
}

// End LucidDbService.java
