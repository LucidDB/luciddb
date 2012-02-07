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
package net.sf.farrago.jdbc;

import java.sql.*;

import java.util.*;

import net.sf.farrago.release.*;

import org.eigenbase.util14.*;


/**
 * FarragoAbstractJdbcDriver is an abstract base for the client and engine sides
 * of the Farrago JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractJdbcDriver
    implements Driver
{
    //~ Methods ----------------------------------------------------------------

    // implement Driver
    public boolean jdbcCompliant()
    {
        // TODO:  true once we pass compliance tests and SQL92 entry level
        return false;
    }

    /**
     * @return the prefix for JDBC URL's understood by this driver
     */
    public abstract String getUrlPrefix();

    /**
     * @return the base JDBC URL for this driver; subclassing drivers can
     * override this to customize the URL scheme
     */
    public String getBaseUrl()
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcUrlBase.get();
    }

    /**
     * @return the JDBC URL interpreted by the engine driver as a connection
     * from an RMI client; subclassing drivers can override this to customize
     * the URL scheme
     */
    public String getClientUrl()
    {
        // NOTE jvs 27-March-2004:  At the moment, the driver interprets
        // embedded and client URL's as the same.  However, we distinguish
        // the actual URL's since in the future we may want to
        // react to them differently.
        return getBaseUrl() + "client_rmi";
    }

    // implement Driver
    public int getMajorVersion()
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcDriverVersionMajor.get();
    }

    // implement Driver
    public int getMinorVersion()
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcDriverVersionMinor.get();
    }

    // implement Driver
    public DriverPropertyInfo [] getPropertyInfo(
        String url,
        Properties info)
        throws SQLException
    {
        // TODO
        return new DriverPropertyInfo[0];
    }

    // implement Driver
    public boolean acceptsURL(String url)
        throws SQLException
    {
        if (url == null) {
            return false;
        }

        if (!url.startsWith(getUrlPrefix())) {
            return false;
        }

        // Make sure we don't accidentally steal a URL intended for
        // an RMI driver which accepts a longer prefix.
        String suffix = url.substring(getUrlPrefix().length());
        if (suffix.startsWith("rmi:")) {
            return false;
        }

        return true;
    }

    /**
     * Indicates whether driver accepts URLs with host:port specification.
     * Returns <code>false</code> by default. Subclassing drivers should
     * override and return true to enable FarragoTestCase to create URLs with
     * host:port specifications.
     *
     * @return false
     */
    public boolean acceptsUrlWithHostPort()
    {
        return false;
    }

    public void register()
    {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println(
                "Error occurred while registering JDBC driver "
                + this + ": " + e.toString());
        }
    }

    /**
     * Returns new Properties object with all input properties and default
     * connection properties. Input properties take precedence over default
     * properties.
     *
     * @param info input properties, copied but unchanged
     *
     * @return new Properties object, never <code>null</code>
     *
     * @see #getDefaultConnectionProps
     */
    public Properties applyDefaultConnectionProps(final Properties info)
    {
        // copy default properties to new properties
        Properties props =
            copyProperties(
                getDefaultConnectionProps(),
                null);

        // copy input properties to new properties
        return copyProperties(info, props);
    }

    /**
     * Returns default connection properties.
     *
     * @return new Properties object, never <code>null</code>
     *
     * @see #applyDefaultConnectionProps
     */
    public Properties getDefaultConnectionProps()
    {
        Properties props = new Properties();

        // default user id is Java user name
        String userName = System.getProperty("user.name");
        if (userName != null) {
            props.setProperty("clientUserName", userName);
        }
        //REVIEW: add default for sessionName?

        // To set process ID, use a shell script to launch the JVM and
        // use "exec" to replace the shell process with the JVM.  This
        // allows you to pass the shell's PID to the JVM:
        //
        //     #!/bin/bash
        //     exec java -Dprocess.id=$$ the.class.name
        // (Without exec, $$ is the bash process's PID.)
        String processId = System.getProperty("process.id");
        if (processId != null) {
            props.setProperty("clientProcessId", processId);
        }

        return props;
    }

    /**
     * Returns destination Properties object after copying source properties
     * into it. Any existing destination property with the same name as a source
     * property is replaced by the source property. A new destination Properties
     * object is created if the input argument is <code>null</code>. Note:
     * Properties object is treated like a Hashtable and all key-value pairs are
     * copied, regardless of whether the keys or values are Strings. Chained
     * Properties objects are OK, but there is no API for accessing non-String
     * values from chained Properties object. So only String values can be
     * copied from chained Properties.
     *
     * @param src source properties, must not be <code>null</code>
     * @param dest destination properties, may be <code>null</code>
     *
     * @return merged properties
     */
    private Properties copyProperties(final Properties src, Properties dest)
    {
        if (dest == null) {
            dest = new Properties();
        }

        // get all keys, including from chained Properties objects
        Enumeration enumer = src.propertyNames();
        while (enumer.hasMoreElements()) {
            // use Hashtable API so can copy non-String keys and values
            Object key = enumer.nextElement();
            Object val = src.get(key);

            // but Hashtable.get() does not search chained Properties,
            // so keys that reference chained properties return null
            if (val == null) {
                if (key instanceof String) {
                    val = src.getProperty((String) key);
                }
                if (val == null) {
                    // item in chained Properties object with
                    // non-String key or value is not accessible
                    continue;
                }
            }
            dest.put(key, val);
        }
        return dest;
    }

    /**
     * Parses params from connection string into {@link Properties} object,
     * returning the stripped URI.
     *
     * @param connectionURI connection string with optional params
     * @param info Properties object; pass <code>null</code> to just get the
     * stripped URI
     *
     * @return connection URI stripped of params; parameters parsed into <code>
     * info</code> if not <code>null</code>.
     *
     * @throws SQLException
     */
    public String parseConnectionParams(String connectionURI, Properties info)
        throws SQLException
    {
        if (connectionURI == null) {
            return null;
        }

        // separate the URI and connection params at the first semicolon
        int i = connectionURI.indexOf(';');
        if (i < 0) {
            return connectionURI;
        }

        String uri = connectionURI.substring(0, i);
        String params = connectionURI.substring(i + 1);

        ConnectStringParser.parse(params, info);

        return uri;
    }
}

// End FarragoAbstractJdbcDriver.java
