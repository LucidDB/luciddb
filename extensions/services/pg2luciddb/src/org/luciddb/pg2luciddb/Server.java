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

package org.luciddb.pg2luciddb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Driver;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.net.InetAddress;
import java.util.Properties;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.h2.util.NetUtils;
import org.h2.util.New;

import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.ScriptReader;

import org.luciddb.pg2luciddb.configuration.Configuration;

/**
 * This class implements a subset of the PostgreSQL protocol as described here:
 * http://developer.postgresql.org/pgdocs/postgres/protocol.html
 * The PostgreSQL catalog is described here:
 * http://www.postgresql.org/docs/7.4/static/catalogs.html
 */

public class Server 
{
    // get logger class:
    private static final Logger logger = Logger.getLogger(Server.class);

    // configuration::
    public Configuration configuration = null;

    // start time:
    public static final long startTime = System.currentTimeMillis();

    private boolean stop;
    private boolean trace;
    private ServerSocket serverSocket;
    private Set<ServerThread> running = Collections.synchronizedSet(new HashSet<ServerThread>());
    private String baseDir;
    private boolean ifExists;    

    // constructor:
    public Server(String[] args) 
    {
        if (args.length > 0)
        {
            try
            {
                configuration = new Configuration(args[0]);
            }
            catch(Exception e)
            {
                logger.error("Can't load configuration from file: " + args[0]);
                logger.debug(e);
            }                        
        }
    }

    // return configuration object:
    public Configuration getConfiguration() 
    {
        return configuration;
    }

    // main entry point:
    public static void main(String[] args) 
    {
       try
       {           
           // ---------------------------------------------------------------------------------------------------
           // disable VJDBC loggers:
           org.apache.log4j.Logger _logger = org.apache.log4j.Logger.getLogger("de.simplicit.vjdbc.VirtualDriver");
           org.apache.log4j.Level _lev = org.apache.log4j.Level.toLevel("OFF");
           _logger.setLevel(_lev);                       
           // disable Zipper logger: 
           _logger = org.apache.log4j.Logger.getLogger("de.simplicit.vjdbc.serial.Zipper");
           _logger.setLevel(_lev);              
           // ---------------------------------------------------------------------------------------------------

           // get logger configuration:
           String propertiesFile = System.getProperty("PG2LucidDB.logger", "conf/log4j.properties");
           PropertyConfigurator.configure(propertiesFile);

           logger.info("Starting PG2LucidDB bridge");

           // runtime hook:
           Runtime.getRuntime().addShutdownHook(new Thread() 
           {
               @Override
               public void run() 
               {
                   logger.info("Exiting PG2LucidDB bridge");
               }
           });

           if (args.length < 1) 
           {
               logger.error("Please supply configuration file");
               System.exit(-1);
           }

           // start the server:  
           Server s = new Server(args);
           s.start();
       }
       catch(Exception ex)
       {
          System.out.println("Exception during starting the server: " + ex);
       }
    }

    /**
     * Remove a thread from the list.
     *
     * @param t the thread to remove
     */
    synchronized void remove(ServerThread t) 
    {
        running.remove(t);
    }

    // get url:
    public String getURL() 
    {
        return "pg://" + NetUtils.getLocalAddress() + ":" + configuration.getServerPort();
    }

    // get listening port:
    public int getPort() 
    {
        return configuration.getServerPort();
    }

    // transforms a CIDR formatted mask into a regular network mask    
    public static String cidrMaskToNetMask(String cidrMask) 
    {
        if (cidrMask == null) 
        {
            return null;
        }
        // Get the integer value of the mask
        int cidrMaskValue = 0;
        try 
        {
            cidrMaskValue = Integer.parseInt(cidrMask);
        } 
        catch (NumberFormatException e) 
        {
            return null;
        }
        int cidrMaskFull = 0xffffffff << (32 - cidrMaskValue);
        int cidrMaskBits1 = cidrMaskFull >> 24 & 0xff;
        int cidrMaskBits2 = cidrMaskFull >> 16 & 0xff;
        int cidrMaskBits3 = cidrMaskFull >> 8 & 0xff;
        int cidrMaskBits4 = cidrMaskFull >> 0 & 0xff;

        StringBuffer netMaskBuf = new StringBuffer();
        netMaskBuf.append(cidrMaskBits1);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits2);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits3);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits4);

        return netMaskBuf.toString();
    }
    
    // applies the given mask to the given IP    
    public static InetAddress applyMask(String ip, String mask) 
    {
        byte[] rawIP = null;
        byte[] rawMask = null;
        try 
        {
            rawIP = InetAddress.getByName(ip).getAddress();
            rawMask = InetAddress.getByName(mask).getAddress();
            if (rawIP.length != rawMask.length) 
            {
                logger.error("IP " + ip + " and mask " + mask + " use different formats");
                return null;
            }
            byte[] maskedAddressBytes = new byte[rawIP.length];
            for (int i = 0; i < rawIP.length; i++) 
            {
                byte currentAddressByte = rawIP[i];
                byte currentMaskByte = rawMask[i];
                maskedAddressBytes[i] = (byte) (currentAddressByte & currentMaskByte);
            }

            return InetAddress.getByAddress(maskedAddressBytes);
        } 
        catch (UnknownHostException uhe) 
        {
            logger.debug("Caught UnknownHostException while applying mask " + mask + " to IP " + ip, uhe);
            return null;
        }
    }

    // check if specified ip is in range:
    public static boolean isInRange(String ip, String ipRange) 
    {
        if (ip == null || ipRange == null)
            return false;

        // separate network part from mask part
        String[] cidrString = ipRange.split("/");
        if (cidrString.length == 0)
            return false;
        String network = cidrString[0];
        String cidrMask = "24";
        // if there is something after '/', that our mask, otherwise, that's a single address
        if (cidrString.length > 1) 
        {
            cidrMask = cidrString[1];
        }

        // Get a regular network mask to apply to the address
        String netMask = cidrMaskToNetMask(cidrMask);
        // Apply it
        InetAddress maskedIP = applyMask(ip, netMask);
        InetAddress maskedNetwork = applyMask(network, netMask);
        if (maskedIP == null || maskedNetwork == null)
            // malformed addresses
            return false;
        return maskedIP.equals(maskedNetwork);
    }

    // check if client can connect to the server:
    private boolean allow(String ip) 
    {
        if (ip == null)
            return false;
 
        // get list of authorized ips:
        List<String> authorizedIPs = configuration.getAuthorizedHosts();    

        if (authorizedIPs == null) {
            return true;
        }
        for (String ipRange : authorizedIPs) 
        {
            if (ipRange != null)
                if (isInRange(ip, ipRange))
                    return true;
        }
        return false;
    }

    // initialize db:
    private boolean initializeDB() throws SQLException 
    {
        boolean res = false;
        Properties credentials = new Properties();
        credentials.put("user", configuration.getDatabaseAdminUsername());
        credentials.put("password", configuration.getDatabaseAdminPassword());
        Connection conn = null;

        Statement stat = null;
        ResultSet rs = null;
        Reader r = null;
        
        try
        {
            conn = configuration.getDriver().connect(configuration.getJdbcDriverBaseURL() + configuration.getJdbcDriverOptions(), credentials);

            rs = conn.getMetaData().getTables(null, "PG_CATALOG", "PG_VERSION", null);
            boolean tableFound = rs.next();
            stat = conn.createStatement();
            if (tableFound) 
            {
                rs = stat.executeQuery("SELECT VERSION FROM PG_CATALOG.PG_VERSION");
                if (rs.next()) 
                {
                    if (rs.getInt(1) == 1) 
                    {
                        res = true;
                        logger.info("pg_catalog schema is already installed");
                    }
                }
                rs.close();
            }
            else
            {
                logger.info("Installing pg_catalog schema...");
                r = new InputStreamReader(new FileInputStream("./install/pg_catalog.sql"));
                ScriptReader reader = new ScriptReader(new BufferedReader(r));
                while (true) 
                {
                   String sql = reader.readStatement();
                   if (sql == null) 
                   {
                       break;
                   }
                   stat.execute(sql);
                }
                reader.close();

                logger.info("pg_catalog schema has been successfully installed");
                res = true;
            }            
        }
        catch(Exception ex)
        {
            logger.error("Exception occured during schema check / initialization: " + ex.toString());
        }        
        finally 
        {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(conn);
            IOUtils.closeSilently(r);
        }

        return res;
    }


    // start the server:
    public void start() throws SQLException
    {
        // initialize db:
        if (!initializeDB())
        {
            return;
        }
        
        // create server socket (no SSL support yet)
        serverSocket = NetUtils.createServerSocket(configuration.getServerPort(), false);
        // listen:
        listen();
    }

    // listen:
    public void listen() 
    {
        String threadName = Thread.currentThread().getName();

        logger.info("Launching server thread with configuration: " + configuration.toString());        

        try 
        {
            while (!stop) 
            {
                Socket s = serverSocket.accept();
                if (!allow(s.getInetAddress().getHostAddress())) 
                {
                    logger.trace("Connection not allowed");
                    s.close();
                } 
                else 
                {
                    ServerThread c = new ServerThread(s, this);
                    running.add(c);
                    c.setProcessId(running.size());
                    Thread thread = new Thread(c);
                    thread.setName(threadName + " thread");
                    c.setThread(thread);
                    thread.start();
                }
            }
        } 
        catch (Exception e) 
        {
            if (!stop) 
            {
                e.printStackTrace();
            }
        }
    }

    // stop:
    public void stop() 
    {
        // TODO server: combine with tcp server
        if (!stop) 
        {
            stop = true;
            if (serverSocket != null) 
            {
                try 
                {
                    serverSocket.close();
                } 
                catch (IOException e) 
                {
                    // TODO log exception
                    e.printStackTrace();
                }
                serverSocket = null;
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        for (ServerThread c : New.arrayList(running)) 
        {
            c.close();
            try 
            {
                Thread t = c.getThread();
                if (t != null) 
                {              
                    t.join(100);
                }
            }  
            catch (Exception e) 
            {
                // TODO log exception
                e.printStackTrace();
            }
        }
    }

    // is running method:
    public boolean isRunning(boolean traceError) 
    {
        if (serverSocket == null) 
        {
            return false;
        }
        try 
        {
            Socket s = NetUtils.createLoopbackSocket(serverSocket.getLocalPort(), false);
            s.close();
            return true;
        } 
        catch (Exception e) 
        {
            if (traceError) 
            {
                traceError(e);
            }
            return false;
        }
    }

    // trace:
    public void trace(String s)
    {
        logger.trace(s);
    }

    // trace error:
    public void traceError(Exception e)
    {
        logger.error(e.getStackTrace());
    }


    /* unused items */
    String getBaseDir() {
        return baseDir;
    }

    public String getType() {
        return "PG";
    }

    public String getName() {
        return "LucidDB PG Server";
    }

    boolean getIfExists() {
        return ifExists;
    }
}
