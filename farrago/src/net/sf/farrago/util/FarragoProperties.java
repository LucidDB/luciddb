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

import net.sf.saffron.util.*;
import net.sf.saffron.util.property.*;

import java.io.*;
import java.util.*;

/**
 * Provides the properties which control limited aspects of Farrago behavior.
 * In most cases, Farrago behavior should be controlled by defining
 * configuration parameters in the catalog, NOT by defining properties here.
 * Java properties should only be used for controlling bootstrap behavior
 * (before the catalog becomes available) or internals which don't belong as
 * parameters (e.g. tweaks for controlling test behavior).  As a gentle hint to
 * keep properties to a minimum, we intentionally make it difficult to set
 * them.  How?  By not defining a Farrago .properties file anywhere.  Instead,
 * runtime and build scripts set just the properties they need on the
 * command line.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoProperties extends Properties
{
    private static FarragoProperties instance;

    /**
     * The string property "net.sf.farrago.home" is the path to the Farrago
     * installation directory.
     */
    public final StringProperty homeDir = new StringProperty(
        this,"net.sf.farrago.home",null);

    /**
     * The optional string property "net.sf.farrago.catalog" is the path to the
     * Farrago catalog directory.  See also {@link #getCatalogDir}
     */
    public final StringProperty catalogDir = new StringProperty(
        this,"net.sf.farrago.catalog",null);

    /**
     * The string property "net.sf.farrago.test.jdbcDriverClass" specifies the
     * fully qualified name of the JDBC driver to use during testing.  If not
     * specified, {@link net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver} is
     * used.
     */
    public final StringProperty testJdbcDriverClass = new StringProperty(
        this,"net.sf.farrago.test.jdbcDriverClass",null);

    /**
     * The boolean property "net.sf.farrago.test.diff" controls whether
     * diffing is actually performed during diff-based testing.  The
     * default is true.
     */
    public final BooleanProperty testDiff = new BooleanProperty(
        this,"net.sf.farrago.test.diff",true);

    /**
     * The string property "net.sf.farrago.fileset.unitsql" specifies a
     * newline-separated list of unit test SQL script files to run.
     */
    public final StringProperty testFilesetUnitsql = new StringProperty(
        this,"net.sf.farrago.fileset.unitsql",null);

    /**
     * The string property "net.sf.farrago.fileset.regressionsql" specifies a
     * newline-separated list of regression test SQL script files to run.
     */
    public final StringProperty testFilesetRegression = new StringProperty(
        this,"net.sf.farrago.fileset.regressionsql",null);

    /**
     * @return the singleton properties object, constructed from
     * {@link System#getProperties}.
     */
    public static synchronized FarragoProperties instance()
    {
        if (instance == null) {
            instance = new FarragoProperties();
        }
        return instance;
    }

    private FarragoProperties()
    {
        super(System.getProperties());
    }

    /**
     * @return the directory containing the Farrago catalog files; equivalent
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
            return new File(homeDir,"catalog");
        }
    }
}

// End FarragoProperties.java
