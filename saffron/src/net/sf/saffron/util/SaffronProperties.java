/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.util;

import openjava.tools.DebugOut;

import java.io.*;

import java.security.AccessControlException;

import java.util.Enumeration;
import java.util.Properties;


/**
 * Provides an environment for debugging
 * information, et cetera, used by saffron.
 * 
 * <p>{@link #getIntProperty} and {@link #getBooleanProperty} are convenience
 * methods.
 * </p>
 * 
 * <p>
 * It is a singleton, accessed via the {@link #instance} method. It is
 * populated from System properties if saffron is invoked via a
 * <code>main()</code> method, from a <code>javax.servlet.ServletContext</code>
 * if saffron is invoked from a servlet, and so forth. If there is a file called
 * <code>"saffron.properties"</code> in the current directory, it is read
 * too.
 * </p>
 * 
 * <p>
 * For each property <code>"saffron.foo.bar"</code> used in saffron code,
 * there should be a String constant called
 * <code>PROPERTY_saffron_foo_bar</code> in this class. The javadoc comment
 * should link to the piece of code which uses that property. (Developers,
 * please make sure that this remains so!)
 * </p>
 */
public class SaffronProperties extends Properties
{
    //~ Static fields/initializers --------------------------------------------

    /** The singleton properties object. */
    private static SaffronProperties properties;

    /** The "{@value}" property is where to compile classes to. */
    public static final String PROPERTY_saffron_class_dir =
        "saffron.class.dir";

    /**
     * If the boolean property "{@value}" is true, {@link
     * openjava.ptree.Statement} prints the statement to {@link System#out}
     * before compiling it.
     */
    public static final String PROPERTY_saffron_Statement_printBeforeCompile =
        "saffron.Statement.printBeforeCompile";

    /**
     * The property "{@value}" is the name of the Java compiler to use. It
     * must implement {@link openjava.ojc.JavaCompiler}. The default value is
     * "{@link #PROPERTY_saffron_java_compiler_class_DEFAULT}".
     */
    public static final String PROPERTY_saffron_java_compiler_class =
        "saffron.java.compiler.class";

    /**
     * Default value for {@link #PROPERTY_saffron_java_compiler_class}
     * ("{@value}").
     */
    public static final String PROPERTY_saffron_java_compiler_class_DEFAULT =
        "JP.ac.tsukuba.openjava.SunJavaCompiler";

    /**
     * The string property "{@value}" is the package in which to include
     * temporary classes. The default is {@link
     * #PROPERTY_saffron_package_name_DEFAULT}.
     */
    public static final String PROPERTY_saffron_package_name =
        "saffron.package.name";

    /**
     * Default value for {@link #PROPERTY_saffron_package_name} ("{@value}").
     */
    public static final String PROPERTY_saffron_package_name_DEFAULT =
        "saffron.runtime";

    /**
     * The string propery "{@value}" is the directory to generate temporary
     * java files to. The default is {@link #PROPERTY_saffron_class_dir the
     * class root}.
     */
    public static final String PROPERTY_saffron_java_dir = "saffron.java.dir";

    /**
     * The string property "{@value}" is the argument string for the {@link
     * #PROPERTY_saffron_java_compiler_class java compiler}. {@link
     * openjava.ojc.JavaCompilerArgs#setString} describes how these arguments
     * are interpreted.
     */
    public static final String PROPERTY_saffron_java_compiler_args =
        "saffron.java.compiler.args";

    /**
     * The boolean property "{@value}" determines whether to optimize variable
     * assignments. If it is true, records are assigned to a variable even if
     * they are never used. Default is false.
     */
    public static final String PROPERTY_saffron_stupid = "saffron.stupid";

    /**
     * The integer property "{@value}" determines how much debugging
     * information is printed. The default, 0, means no debugging.
     */
    public static final String PROPERTY_saffron_debug_level =
        "saffron.debug.level";

    /**
     * The string property "{@value}" is the name of the file to send
     * debugging information to. <code>"out"</code> (the default), means send
     * to {@link System#out}; <code>"err"</code> means send to {@link
     * System#err}.
     */
    public static final String PROPERTY_saffron_debug_out =
        "saffron.debug.out";

    /**
     * The string property "{@value}" is used by {@link
     * net.sf.saffron.test.Main#suite}.
     */
    public static final String PROPERTY_saffron_test_Name =
        "saffron.test.Name";

    /**
     * The string property "{@value}" is used by {@link
     * net.sf.saffron.test.Main#suite}.
     */
    public static final String PROPERTY_saffron_test_Class =
        "saffron.test.Class";

    /**
     * The string property "{@value}" is used by {@link
     * net.sf.saffron.test.Main#suite}.
     */
    public static final String PROPERTY_saffron_test_Suite =
        "saffron.test.Suite";

    /**
     * The string property "{@value}" is used by
     * {@link net.sf.saffron.test.Main#suite}.
     */
    public static final String PROPERTY_saffron_test_everything =
        "saffron.test.everything";

    /**
     * The string property "{@value}" is the URL of the JDBC database which
     * contains the EMP and DEPT tables used for testing.
     */
    public static final String PROPERTY_saffron_test_jdbc_url =
        "saffron.test.jdbc.url";

    /**
     * The string property "{@value}" is a comma-separated list of class names
     * to be used as JDBC drivers.
     */
    public static final String PROPERTY_saffron_test_jdbc_drivers =
        "saffron.test.jdbc.drivers";

    /**
     * The boolean property "{@value}" determines whether the optimizer will
     * consider adding converters of infinite cost in order to convert a
     * relational expression from one calling convention to another. The default
     * value is <code>true</code>.
     */
    public static final String PROPERTY_saffron_opt_allowInfiniteCostConverters =
        "saffron.opt.allowInfiniteCostConverters";

    //~ Constructors ----------------------------------------------------------

    /**
     * Please use {@link #instance} to create a {@link SaffronProperties}.
     */
    private SaffronProperties()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Retrieves the singleton instance of {@link SaffronProperties}.
     */
    public static SaffronProperties instance()
    {
        if (properties == null) {
            properties = new SaffronProperties();

            // read properties from the file "saffron.properties", if it exists
            File file = new File("saffron.properties");
            try {
                if (file.exists()) {
                    try {
                        properties.load(new FileInputStream(file));
                    } catch (IOException e) {
                        throw Util.newInternal(e,"while reading from " + file);
                    }
                }
            } catch (AccessControlException e) {
                // we're in a sandbox
            }

            // copy in all system properties which start with "saffron."
            for (
                Enumeration keys = System.getProperties().keys();
                    keys.hasMoreElements();) {
                String key = (String) keys.nextElement();
                String value = System.getProperty(key);
                if (key.startsWith("saffron.")) {
                    properties.setProperty(key,value);
                }
            }
        }
        return properties;
    }

    /**
     * Retrieves a boolean property. Returns <code>true</code> if the property
     * exists, and its value is <code>1</code>, <code>true</code> or
     * <code>yes</code>; returns <code>false</code> otherwise.
     */
    public boolean getBooleanProperty(String key)
    {
        return getBooleanProperty(key,false);
    }

    /**
     * Retrieves a boolean property, or a default value if the property does
     * not exist. Returns <code>true</code> if the property
     * exists, and its value is <code>1</code>, <code>true</code> or
     * <code>yes</code>; the default value if it does not exist;
     * <code>false</code> otherwise.
     */
    public boolean getBooleanProperty(String key, boolean defaultValue)
    {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value.equalsIgnoreCase("1") || value.equalsIgnoreCase("true")
            || value.equalsIgnoreCase("yes");
    }

    /**
     * Retrieves an integer property. Returns -1 if the property is not found,
     * or if its value is not an integer.
     */
    public int getIntProperty(String key)
    {
        String value = getProperty(key);
        if (value == null) {
            return -1;
        }
        int i = Integer.valueOf(value).intValue();
        return i;
    }

    /**
     * Applies properties to the right locations.
     */
    public void apply()
    {
        int debugLevel = getIntProperty(PROPERTY_saffron_debug_level);
        String debugOut = getProperty(PROPERTY_saffron_debug_out);
        if (debugLevel >= 0) {
            DebugOut.setDebugLevel(debugLevel);
            if ((debugOut == null) || debugOut.equals("")) {
                debugOut = "out";
            }
        }
        if ((debugOut != null) && !debugOut.equals("")) {
            if (debugOut.equals("err")) {
                DebugOut.setDebugOut(System.err);
            } else if (debugOut.equals("out")) {
                DebugOut.setDebugOut(System.out);
            } else {
                try {
                    File file = new File(debugOut);
                    PrintStream ps =
                        new PrintStream(new FileOutputStream(file),true);
                    DebugOut.setDebugOut(ps);
                } catch (FileNotFoundException e) {
                    throw Util.newInternal(e,"while setting debug output");
                }
            }
        }
    }
}


// End SaffronProperties.java
