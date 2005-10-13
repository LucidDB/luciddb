/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.plugin;

import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import java.net.*;
import java.util.*;
import java.util.jar.*;

/**
 * FarragoPluginClassLoader allows plugin jars to be added to the
 * ClassLoader dynamically.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPluginClassLoader extends URLClassLoader
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Prefix used to indicate that a wrapper library is loaded directly from
     * a class rather than a jar.
     */
    public static final String LIBRARY_CLASS_PREFIX = "class ";

    /**
     * Attribute name used in jar manifest for identifying the class
     * to be used as a plugin factory.
     */
    public static final String PLUGIN_FACTORY_CLASS_ATTRIBUTE
        = "PluginFactoryClassName";

    /**
     * Attribute name used in jar manifest for identifying the XMI file
     * to be used as a model extension.
     */
    public static final String PLUGIN_MODEL_ATTRIBUTE
        = "PluginModel";

    //~ Instance fields -------------------------------------------------------
    
    private final Set urlSet;
    
    public FarragoPluginClassLoader()
    {
        super(new URL[0]);
        urlSet = new HashSet();
    }

    /**
     * Adds a URL from which plugins can be loaded.
     *
     * @param url URL to add
     */
    public void addPluginUrl(URL url)
    {
        if (urlSet.contains(url)) {
            return;
        }
        addURL(url);
        urlSet.add(url);
    }
    
    /**
     * Loads a Java class from a library (either a jarfile using
     * a manifest to get the classname, or else a named
     * class from the classpath).
     *
     * @param libraryName filename of jar containing plugin implementation
     *
     * @param jarAttributeName name of jar attribute to use to determine
     * class name
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

            if (libraryName.startsWith(LIBRARY_CLASS_PREFIX)) {
                String className =
                    libraryName.substring(LIBRARY_CLASS_PREFIX.length());
                return Class.forName(className);
            } else {
                JarFile jar = new JarFile(libraryName);
                Manifest manifest = jar.getManifest();
                String className =
                    manifest.getMainAttributes().getValue(jarAttributeName);
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

    /**
     * Loads a Java class from a jar URL, using the manifest to
     * determine the classname.
     *
     * @param jarUrl URL of jar containing plugin implementation
     *
     * @param jarAttributeName name of jar attribute to use to determine
     * class name
     *
     * @return loaded class
     */
    public Class loadClassFromJarUrlManifest(
        String jarUrl,
        String jarAttributeName)
    {
        jarUrl =
            FarragoProperties.instance().expandProperties(jarUrl);
        String className;
        try {
            URL url = new URL("jar:" + jarUrl + "!/");
            JarURLConnection jarConnection = (JarURLConnection)
                url.openConnection();
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
     *
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
     * thread's context ClassLoader is set to <code>this</code> for the
     * duration of the construction.
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
}

// End FarragoPluginClassLoader.java
