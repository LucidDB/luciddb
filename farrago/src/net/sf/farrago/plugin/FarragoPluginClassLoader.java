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
package net.sf.farrago.plugin;

import java.io.*;
import java.net.*;

import java.util.*;
import java.util.jar.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.*;

/**
 * FarragoPluginClassLoader allows plugin jars to be added to the ClassLoader
 * dynamically.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPluginClassLoader
    extends URLClassLoader
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getRuntimeContextTracer();

    /**
     * Prefix used to indicate that a wrapper library is loaded directly from a
     * class rather than a jar. TODO: get rid of this and use only
     * LIBRARY_CLASS_PREFIX2.
     */
    public static final String LIBRARY_CLASS_PREFIX1 = "class ";

    /**
     * New alternative to LIBRARY_CLASS_PREFIX1.
     */
    public static final String LIBRARY_CLASS_PREFIX2 = "class:";

    /**
     * Attribute name used in jar manifest for identifying the class to be used
     * as a plugin factory.
     */
    public static final String PLUGIN_FACTORY_CLASS_ATTRIBUTE =
        "PluginFactoryClassName";

    /**
     * Attribute name used in jar manifest for identifying the XMI file to be
     * used as a model extension.
     */
    public static final String PLUGIN_MODEL_ATTRIBUTE = "PluginModel";

    //~ Instance fields --------------------------------------------------------

    private final Set urlSet;

    //~ Constructors -----------------------------------------------------------

    public FarragoPluginClassLoader()
    {
        // NOTE jvs 8-Aug-2006:  use context classloader to make
        // Farrago work correctly within a J2EE app server.
        super(new URL[0], Thread.currentThread().getContextClassLoader());
        urlSet = new HashSet();
    }

    public FarragoPluginClassLoader(ClassLoader parentClassLoader)
    {
        super(new URL[0], parentClassLoader);
        urlSet = new HashSet();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a URL from which plugins can be loaded.
     *
     * @param url URL to add
     */
    public void addPluginUrl(URL url)
    {
        // Canonicalize local filenames to reduce the chance of
        // loading two copies of the same jar
        if (url.getProtocol().equals("file")) {
            try {
                String libraryName = url.getFile();
                File libraryFile = new File(libraryName);
                libraryName = libraryFile.getCanonicalPath();
                url = new URL("file", null, libraryName);
            } catch (Throwable ex) {
                throw Util.newInternal(ex);
            }
        }
        if (urlSet.contains(url)) {
            return;
        }
        addURL(url);
        urlSet.add(url);
    }

    /**
     * Loads a Java class from a library (either a jarfile using a manifest to
     * get the classname, or else a named class from the classpath).
     *
     * @param libraryName filename of jar containing plugin implementation
     * @param jarAttributeName name of jar attribute to use to determine class
     * name
     *
     * @return loaded class
     */
    public Class loadClassFromLibraryManifest(
        String libraryName,
        String jarAttributeName)
    {
        try {
            libraryName =
                FarragoProperties.instance().expandProperties(libraryName);

            if (isLibraryClass(libraryName)) {
                String className = getLibraryClassReference(libraryName);
                return Class.forName(className);
            } else {
                JarFile jar = new JarFile(libraryName);
                Manifest manifest = jar.getManifest();
                String className =
                    manifest.getMainAttributes().getValue(jarAttributeName);
                if (className == null) {
                    throw FarragoResource.instance().PluginManifestMissing.ex(
                        libraryName,
                        jarAttributeName);
                }
                return loadClassFromJarUrl(
                    "file:" + libraryName,
                    className);
            }
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginJarLoadFailed.ex(
                libraryName,
                ex);
        }
    }

    protected Class<?> findClass(String name)
        throws ClassNotFoundException
    {
        try {
            return super.findClass(name);
        } catch (ClassNotFoundException cnfe) {
            return getParent().loadClass(name);
        }
    }

    /**
     * Loads a Java class from a jar URL, using the manifest to determine the
     * classname.
     *
     * @param jarUrl URL of jar containing plugin implementation
     * @param jarAttributeName name of jar attribute to use to determine class
     * name
     *
     * @return loaded class
     */
    public Class loadClassFromJarUrlManifest(
        String jarUrl,
        String jarAttributeName)
    {
        if (isLibraryClass(jarUrl)) {
            try {
                String className = getLibraryClassReference(jarUrl);
                return Class.forName(className);
            } catch (Throwable ex) {
                throw FarragoResource.instance().PluginJarLoadFailed.ex(
                    jarUrl,
                    ex);
            }
        }
        jarUrl = FarragoProperties.instance().expandProperties(jarUrl);
        String className;
        try {
            URL url = new URL("jar:" + jarUrl + "!/");
            JarURLConnection jarConnection =
                (JarURLConnection) url.openConnection();
            Manifest manifest = jarConnection.getManifest();
            className = manifest.getMainAttributes().getValue(jarAttributeName);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginJarLoadFailed.ex(
                jarUrl,
                ex);
        }
        return loadClassFromJarUrl(jarUrl, className);
    }

    /**
     * Loads a Java class from a jar URL.
     *
     * @param jarUrl URL for jar containing class implementation
     * @param className name of class to load
     *
     * @return loaded class
     */
    public Class loadClassFromJarUrl(
        String jarUrl,
        String className)
    {
        try {
            URL url = new URL(jarUrl);
            addPluginUrl(url);
            return loadClass(className);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginJarClassLoadFailed.ex(
                className,
                jarUrl,
                ex);
        }
    }

    /**
     * Constructs a new object instance of a plugin class, making sure the
     * thread's context ClassLoader is set to <code>this</code> for the duration
     * of the construction.
     *
     * @param pluginClass class to instantiate
     *
     * @return new object
     */
    public Object newPluginInstance(Class pluginClass)
        throws InstantiationException, IllegalAccessException
    {
        Thread currentThread = Thread.currentThread();
        ClassLoader savedClassLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(this);
            return pluginClass.newInstance();
        } finally {
            currentThread.setContextClassLoader(savedClassLoader);
        }
    }

    /**
     * Tests whether a library name references a Java class directly rather than
     * a jar.
     *
     * @param libraryName library name to be tested
     *
     * @return true iff libraryName references a Java class
     */
    public static boolean isLibraryClass(String libraryName)
    {
        return libraryName.startsWith(LIBRARY_CLASS_PREFIX1)
            || libraryName.startsWith(LIBRARY_CLASS_PREFIX2);
    }

    /**
     * From a library name which references a Java class directly, obtains the
     * class reference (possibly with trailing context such as a method name).
     *
     * @param libraryName library name to be parsed
     *
     * @return class name
     */
    public static String getLibraryClassReference(String libraryName)
    {
        assert (isLibraryClass(libraryName));
        return libraryName.substring(LIBRARY_CLASS_PREFIX2.length());
    }
}

// End FarragoPluginClassLoader.java
