/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.util;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.eigenbase.util.*;
import org.eigenbase.util.property.*;


/**
 * Provides the properties which control limited aspects of Farrago behavior.
 * In most cases, Farrago behavior should be controlled by defining
 * configuration parameters in the catalog, NOT by defining properties here.
 * Java properties should only be used for controlling bootstrap behavior
 * (before the catalog becomes available) or internals which don't belong as
 * parameters (e.g. tweaks for controlling test behavior).  As a gentle hint to
 * keep properties to a minimum, we intentionally make it difficult to set
 * them.  How?  By not defining a master Farrago .properties file.  Instead,
 * runtime and build scripts set just the properties they need on the
 * command line.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoProperties extends Properties
{
    //~ Static fields/initializers --------------------------------------------

    private static FarragoProperties instance;
    private static final String PROPERTY_EXPANSION_PATTERN = "\\$\\{\\w+\\}";

    //~ Instance fields -------------------------------------------------------

    /**
     * The string property "net.sf.farrago.home" is the path to the Farrago
     * installation directory.
     */
    public final StringProperty homeDir =
        new StringProperty(this, "net.sf.farrago.home", null);

    /**
     * The optional string property "net.sf.farrago.catalog" is the path to the
     * Farrago repository directory.  See also {@link #getCatalogDir}
     */
    public final StringProperty catalogDir =
        new StringProperty(this, "net.sf.farrago.catalog", null);

    /**
     * The optional string property
     * "net.sf.farrago.defaultSessionFactoryLibraryName" is the name
     * of the plugin library to use for the default session factory.
     * Defaults to {@link net.sf.farrago.defimpl.FarragoDefaultSessionFactory}.
     */
    public final StringProperty defaultSessionFactoryLibraryName =
        new StringProperty(
            this,
            "net.sf.farrago.defaultSessionFactoryLibraryName",
            "class net.sf.farrago.defimpl.FarragoDefaultSessionFactory");

    /**
     * The string property "net.sf.farrago.test.jdbcDriverClass" specifies the
     * fully qualified name of the JDBC driver to use during testing.  If not
     * specified, {@link net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver} is
     * used.
     */
    public final StringProperty testJdbcDriverClass =
        new StringProperty(this, "net.sf.farrago.test.jdbcDriverClass", null);

    /**
     * The string property "net.sf.farrago.fileset.unitsql" specifies a
     * newline-separated list of unit test SQL script files to run.
     */
    public final StringProperty testFilesetUnitsql =
        new StringProperty(this, "net.sf.farrago.fileset.unitsql", null);

    /**
     * The string property "net.sf.farrago.fileset.regressionsql" specifies a
     * newline-separated list of regression test SQL script files to run.
     */
    public final StringProperty testFilesetRegression =
        new StringProperty(this, "net.sf.farrago.fileset.regressionsql", null);

    /**
     * The string property "net.sf.farrago.fileset.concurrentsql" specifies a
     * newline-separated list of concurrency test SQL script files to run.
     */
    public final StringProperty testFilesetConcurrent =
        new StringProperty(this, "net.sf.farrago.fileset.concurrentsql", null);

    //~ Constructors ----------------------------------------------------------

    private FarragoProperties()
    {
        super(System.getProperties());
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} properties object, constructed from {@link
     * System#getProperties}.
     */
    public static synchronized FarragoProperties instance()
    {
        if (instance == null) {
            instance = new FarragoProperties();
        }
        return instance;
    }

    /**
     * @return the directory containing the Farrago repos files; equivalent
     * to {@link #catalogDir} if set, otherwise the "catalog" subdirectory of
     * {@link #homeDir}
     */
    public File getCatalogDir()
    {
        String catalogDirOpt = instance().catalogDir.get();
        if (catalogDirOpt != null) {
            return new File(catalogDirOpt);
        } else {
            String homeDir = instance().homeDir.get(true);
            return new File(homeDir, "catalog");
        }
    }

    // REVIEW: SZ: 7/20/2004: Add support for expanding any of the
    // above properties?  Maybe just come up with a well-defined set
    // of FarragoProperties fields that should be expandable: homeDir,
    // catalogDir, etc.  Also, the definition of a property name
    // should probably be expanded to include more punctuation and/or
    // non-ASCII letters.

    /**
     * Expands properties embedded in the given String.  Property
     * names are encoded as in Ant: <code>${propertyName}</code>.
     * Property names must match the {@link Pattern} \w character
     * class (<code>[a-zA-z_0-9]</code>).  References to unknown
     * properties are not modified (e.g., the expansion of
     * <code>"${UNKNOWN}"</code> is <code>"${UNKNOWN}"</code>).
     *
     * <p>Currently, the only supported properties are
     *
     *<ul>
     *<li><code>${FARRAGO_HOME}</code>:  replaced with the value
     * of {@link #homeDir}.
     *<li><code>${FARRAGO_CATALOG_DIR}</code>:  replaced with the value
     * of {@link #getCatalogDir()}.
     *</ul>
     *
     * @param value a value that may or may not contain property names
     *              to be expanded.
     * @return the <code>value</code> parameter with its property
     *         references expanded -- returns <code>value</code> if no
     *         known property references are found
     */
    public String expandProperties(String value)
    {
        Pattern patt = Pattern.compile(PROPERTY_EXPANSION_PATTERN);

        Matcher matcher = patt.matcher(value);

        StringBuffer result = null;
        int offset = 0;
        while (matcher.find()) {
            int start = matcher.start();
            int end = matcher.end();

            String propertyName = value.substring(start + 2, end - 1);

            String replacement = null;
            if (propertyName.equals("FARRAGO_HOME")) {
                replacement = homeDir.get();
            } else if (propertyName.equals("FARRAGO_CATALOG_DIR")) {
                replacement = getCatalogDir().getAbsolutePath();
            }
            
            if (replacement != null) {
                if (result == null) {
                    result = new StringBuffer(value);
                }

                result.replace(start + offset, end + offset, replacement);
                offset += (replacement.length() - (end - start));
            }
        }

        if (result != null) {
            return result.toString();
        }

        return value;
    }
}


// End FarragoProperties.java
