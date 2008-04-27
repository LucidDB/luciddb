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
