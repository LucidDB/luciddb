/*
// $Id$
// pg2luciddb is a PG emulator for LucidDB
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
// Portions Copyright (C) 2009 Alexander Mekhrishvili
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

package org.luciddb.pg2luciddb.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Hashtable;

import org.apache.log4j.Logger;

public class Configuration 
{
    // get logger:
    private static final Logger logger = Logger.getLogger(Configuration.class);

    // server port. 9999 by default
    private int serverPort;

    // jdbc driver url:
    private String jdbcDriverBaseURL;

    // jdbc driver options:
    private String jdbcDriverOptions;

    // close the connection after was idle for the specified period (in miliseconds)
    // a value of zero is interpreted as an infinite timeout.
    private int connectionCloseIdleTimeout;

    // JDBC fetchsize hint:
    private int fetchSize;
   
    // list of hosts allowed to connect to us - null means any host, not null means only these ones
    private List<String> authorizedHosts = null;
    
    // Hashtable with allowed users:
    private Hashtable allowedUsers = null;
    
    // jdbc driver class name:
    private String jdbcDriverClassName;
    
    // driver:
    private Driver driver;

    // confuguration filename:
    private String configurationFilename;

    // database admin's username:
    private String databaseAdminUsername;

    // database admin's password:
    private String databaseAdminPassword;

    // jdbc driver options file:
    private Hashtable jdbcDriverOptionsForUsers = null;

    // constructor:
    public Configuration(String configurationFilename) throws IOException, ClassNotFoundException,
            IllegalAccessException, InstantiationException 
    {
    	// set configuration filename:
        this.configurationFilename = configurationFilename;
        // open file:
        FileInputStream fis = new FileInputStream(new File(configurationFilename));
        // read all properties:
        Properties prop = new Properties();
        prop.load(fis);

        // get server port:
        serverPort = Integer.parseInt(prop.getProperty("serverPort", "9999").trim());
        // get jdbc driver url:
        jdbcDriverBaseURL = prop.getProperty("jdbcDriverBaseUrl", "jdbc:luciddb:rmi://localhost").trim();
        // get jdbc driver options:
        jdbcDriverOptions = prop.getProperty("jdbcDriverOptions", "").trim();
        // get connection close idle timeout:
        connectionCloseIdleTimeout = Integer.parseInt(prop.getProperty("connectionCloseIdleTimeout", "0").trim());
        // get fetch size:
        fetchSize = Integer.parseInt(prop.getProperty("statementFetchSize", "0").trim());
        // database admin's username:
        databaseAdminUsername = prop.getProperty("databaseAdminUsername", "sa");
        // database admin's password:
        databaseAdminPassword = prop.getProperty("databaseAdminPassword", "");        

        // load the JDBC driver
        String jdbcDriverClassname = prop.getProperty("jdbcDriver", "com.lucidera.jdbc.LucidDbRmiDriver").trim();
        try 
        {
            driver = (Driver) Class.forName(jdbcDriverClassname).newInstance();
        } 
        catch (ClassNotFoundException e) 
        {
            logger.error("Could not find class " + jdbcDriverClassname);
            throw e;
        }
                
        // get allowed users:
        String allowedUsersFile = prop.getProperty("allowedUsersFile", "../conf/allowed_users").trim();
        // open file:
        FileInputStream fis2 = new FileInputStream(new File(allowedUsersFile));
        Properties p = new Properties();
        p.load(fis2);
        allowedUsers = (Hashtable)p;
        // close stream:
        try
        {
            fis2.close();
        }
        catch(Exception ex) { }

        // get jdbc driver options for users:
        String _jdbcDriverOptionsFile = prop.getProperty("jdbcDriverOptionsFile", "../conf/driver_options").trim();
        try
        {
            FileInputStream fis3 = new FileInputStream(new File(_jdbcDriverOptionsFile));
            Properties p2 = new Properties();
            p2.load(fis3);
            jdbcDriverOptionsForUsers = (Hashtable)p2;
            // close stream:
            try
            {
                fis3.close();
            }
            catch(Exception e) { }
        }
        catch(Exception ex) { }
        
        // get the authorized hosts
        String authorizedHostsFile = prop.getProperty("authorizedHostsFile", "../conf/authorized_hosts");
        BufferedReader input = null;
        try 
        {
            input = new BufferedReader(new FileReader(authorizedHostsFile));
            authorizedHosts = new ArrayList<String>();
            String line = null;
            while ((line = input.readLine()) != null) 
            {
                line = line.trim();
                if (!line.startsWith("#") && line.length() > 0)
                    authorizedHosts.add(line);
            }
        } 
        catch (IOException e) 
        {
            logger.warn("No or unreadable authorized hosts file " + authorizedHostsFile
                    + " - NOT using IP authentication");
            authorizedHosts = null;
        } 
        finally 
        {
            if (input != null)
            {
                try 
                {
                    input.close();
                } 
                catch (IOException ignored) { }
            }
        }
    }

    // get database admin's username:
    public String getDatabaseAdminUsername()
    {
        return databaseAdminUsername;
    }

    // get database admin's password:
    public String getDatabaseAdminPassword()
    {
        return databaseAdminPassword;
    }

    // get server port:
    public int getServerPort() 
    {
        return serverPort;
    }

    // get jdbc driver:
    public Driver getDriver() 
    {
        return driver;
    }

    // get jdbc driver base url:
    public String getJdbcDriverBaseURL() 
    {
        return jdbcDriverBaseURL;
    }

    // get jdbc driver class name:
    public String getJdbcDriverClassName() 
    {
        return jdbcDriverClassName;
    }

    // get jdbc driver options:
    public String getJdbcDriverOptions() 
    {
        return jdbcDriverOptions;
    }

    // get connection close idle timeout:
    public int getConnectionCloseIdleTimeout() 
    {
        return connectionCloseIdleTimeout;
    }

    // get fetch size:
    public int getFetchSize() 
    {
        return fetchSize;
    }

    // get list of authorized hosts:
    public List<String> getAuthorizedHosts() 
    {
        return authorizedHosts;
    }
    
    // get hashtable with allowed users:
    public Hashtable getAllowedUsers()
    {
    	return allowedUsers;
    }

    // get jdbc driver options for users:
    public Hashtable getJdbcDriverOptionsForUsers()
    {
        return jdbcDriverOptionsForUsers;
    }

    // get jdbc driver options for specified user:
    public String getJdbcDriverOptionsForUser(String userName)
    {
        if (jdbcDriverOptionsForUsers != null && jdbcDriverOptionsForUsers.get(userName) != null)
        {
            return jdbcDriverOptionsForUsers.get(userName).toString();
        }
        return jdbcDriverOptions;
    }
    
    @Override
    public String toString() 
    {
        return configurationFilename + " port: " + serverPort;
    }
}
