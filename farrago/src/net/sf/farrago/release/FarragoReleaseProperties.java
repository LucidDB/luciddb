/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package net.sf.farrago.release;

import java.io.*;

import java.net.*;

import java.util.*;

import org.eigenbase.util.property.*;


/**
 * Provides immutable properties burned into a particular release of Farrago.
 * See {@link net.sf.farrago.util.FarragoProperties} for an explanation of what
 * NOT to define here. In addition, no site-specific property should ever be
 * defined here.
 *
 * <p>Products and projects which embed or rebrand Farrago rely on being able to
 * control this information in a self-contained location (typically a file named
 * FarragoRelease.properties in the root of the release jar).
 *
 * <p>Note that the default values defined in this class are not appropriate for
 * real releases; they are only defined to allow developer builds to run without
 * a properties file.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReleaseProperties
    extends Properties
{
    //~ Static fields/initializers ---------------------------------------------

    private static FarragoReleaseProperties instance;

    //~ Instance fields --------------------------------------------------------

    /**
     * Base name of the released package for package management systems such as
     * rpm and deb.
     */
    public final StringProperty packageName =
        new StringProperty(this, "package.name", "farrago");

    /**
     * Name of the product (e.g. as reported via java.sql.DatabaseMetaData)
     */
    public final StringProperty productName =
        new StringProperty(this, "product.name", "Farrago");

    /**
     * Major version number for the product.
     */
    public final IntegerProperty productVersionMajor =
        new IntegerProperty(this, "product.version.major", 0);

    /**
     * Minor version number for the product.
     */
    public final IntegerProperty productVersionMinor =
        new IntegerProperty(this, "product.version.minor", 1);

    /**
     * Point-release version number for the product.
     */
    public final IntegerProperty productVersionPoint =
        new IntegerProperty(this, "product.version.point", 0);

    /**
     * Name of the JDBC driver as reported via java.sql.DatabaseMetaData.
     */
    public final StringProperty jdbcDriverName =
        new StringProperty(this, "jdbc.driver.name", "FarragoJdbcDriver");

    /**
     * JDBC driver major version number as reported via
     * java.sql.DatabaseMetaData.
     */
    public final IntegerProperty jdbcDriverVersionMajor =
        new IntegerProperty(this, "jdbc.driver.version.major", 0);

    /**
     * JDBC driver minor version number as reported via
     * java.sql.DatabaseMetaData.
     */
    public final IntegerProperty jdbcDriverVersionMinor =
        new IntegerProperty(this, "jdbc.driver.version.minor", 1);

    /**
     * Base string for JDBC connection URL.
     */
    public final StringProperty jdbcUrlBase =
        new StringProperty(this, "jdbc.url.base", "jdbc:farrago:");

    /**
     * Default port to use for JDBC connections. Note that this is the
     * release-level default; the actual port to use for a server can be
     * overridden at each site via system parameter serverRmiRegistryPort, and
     * for a client by explicitly including the port in the connection URL.
     */
    public final IntegerProperty jdbcUrlPortDefault =
        new IntegerProperty(this, "jdbc.url.port.default", 5433);

    /**
     * Default port to use for JDBC connections over HTTP. Note that this is
     * the release-level default; the actual port to use for a server can be
     * overridden at each site through app server or catalog settings, and for
     * a client by explicitly including the port in the connection URL.
     */
    public final IntegerProperty jdbcUrlHttpPortDefault =
        new IntegerProperty(this, "jdbc.url.http.port.default", 8033);

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} properties object
     */
    public static synchronized FarragoReleaseProperties instance()
    {
        if (instance == null) {
            String resourceName = "FarragoRelease.properties";
            String failureString = "Failed to load " + resourceName;
            instance = new FarragoReleaseProperties();
            URL url =
                FarragoReleaseProperties.class.getClassLoader().getResource(
                    resourceName);
            if (url == null) {
                throw new RuntimeException(failureString);
            }
            InputStream urlStream = null;
            try {
                urlStream = url.openStream();
                instance.load(urlStream);
            } catch (IOException ex) {
                RuntimeException rx = new RuntimeException(failureString);
                rx.initCause(ex);
                throw rx;
            } finally {
                if (urlStream != null) {
                    try {
                        urlStream.close();
                    } catch (IOException ex) {
                        // ignore
                    }
                }
            }
        }
        return instance;
    }
}

// End FarragoReleaseProperties.java
