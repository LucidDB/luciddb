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
package net.sf.farrago.plugin;

import java.net.*;
import java.util.*;
import java.util.jar.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;


/**
 * FarragoPluginCache is an abstract private cache for loading instances of
 * {@link FarragoPlugin} (and their component sub-objects).  It requires an
 * underlying shared {@link FarragoObjectCache}.
 *
 *<p>
 *
 * This class is only a partial implementation.  For an example of how to build
 * a full implementation, see {@link
 * net.sf.farrago.namespace.util.FarragoDataWrapperCache}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoPluginCache extends FarragoCompoundAllocation
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Prefix used to indicate that a wrapper library is loaded directly from
     * a class rather than a JAR.
     */
    public static final String LIBRARY_CLASS_PREFIX = "class ";

    //~ Instance fields -------------------------------------------------------

    private Map mapMofIdToPlugin;
    private FarragoObjectCache sharedCache;
    private FarragoRepos repos;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty cache.
     *
     * @param owner FarragoAllocationOwner for this cache, to make sure
     * everything gets discarded eventually
     *
     * @param sharedCache underlying shared cache
     *
     * @param repos FarragoRepos for wrapper initialization
     */
    public FarragoPluginCache(
        FarragoAllocationOwner owner,
        FarragoObjectCache sharedCache,
        FarragoRepos repos)
    {
        owner.addAllocation(this);
        this.sharedCache = sharedCache;
        this.repos = repos;
        mapMofIdToPlugin = new HashMap();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the underlying repos
     */
    public FarragoRepos getRepos()
    {
        return repos;
    }

    /**
     * @return the underlying shared cache
     */
    public FarragoObjectCache getSharedCache()
    {
        return sharedCache;
    }

    /**
     * Searches this cache for a plugin object identified by its catalog MofId.
     *
     * @param mofId MofId of plugin object being loaded
     *
     * @return cached instance or null if not yet cached
     */
    protected Object searchPrivateCache(String mofId)
    {
        return mapMofIdToPlugin.get(mofId);
    }

    /**
     * Adds a plugin object to this cache.
     *
     * @param entry pinned entry from underlying shared cache;
     * key must be plugin object's catalog MofId
     *
     * @return cached plugin object
     */
    protected Object addToPrivateCache(FarragoObjectCache.Entry entry)
    {
        // take ownership of the pinned cache entry
        addAllocation(entry);

        Object obj = entry.getValue();
        mapMofIdToPlugin.put(
            entry.getKey(),
            obj);
        return obj;
    }

    /**
     * Loads the Java class implementing a plugin.
     *
     * @param libraryName name of library containing plugin implementation
     *
     * @param jarAttributeName name of JAR attribute to use to determine
     * class name
     *
     * @return loaded class
     */
    public static Class loadPluginClass(
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
                return loadJarClass(
                    "file:" + libraryName,
                    className);
            }
        } catch (Throwable ex) {
            throw FarragoResource.instance().newPluginJarLoadFailed(
                libraryName,
                ex);
        }
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
    public static Class loadJarClass(
        String jarUrl,
        String className)
    {
        try {
            URL url = new URL(jarUrl);
            URLClassLoader classLoader =
                new URLClassLoader(new URL [] { url },
                    FarragoPluginCache.class.getClassLoader());
            return classLoader.loadClass(className);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newPluginJarClassLoadFailed(
                className,
                jarUrl,
                ex);
        }
    }

    /**
     * Initializes a plugin instance.
     *
     * @param libraryName name of library containing plugin implementation
     *
     * @param jarAttributeName name of JAR attribute to use to determine
     * class name
     *
     * @param options options with which to initialize plugin
     *
     * @return initialized plugin
     */
    protected FarragoPlugin initializePlugin(
        String libraryName,
        String jarAttributeName,
        Properties options)
    {
        Class pluginClass = loadPluginClass(libraryName, jarAttributeName);

        FarragoPlugin plugin;
        try {
            Object obj = pluginClass.newInstance();
            assert(obj instanceof FarragoPlugin) : obj.getClass();
            plugin = (FarragoPlugin) obj;
            plugin.initialize(repos, options);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newPluginInitFailed(
                repos.getLocalizedObjectName(libraryName),
                ex);
        }
        return plugin;
    }
}


// End FarragoPluginCache.java
