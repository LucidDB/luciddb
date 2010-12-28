/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;


/**
 * FarragoPluginCache is an abstract private cache for loading instances of
 * {@link FarragoPlugin} (and their component sub-objects). It requires an
 * underlying shared {@link FarragoObjectCache}.
 *
 * <p>This class is only a partial implementation. For an example of how to
 * build a full implementation, see {@link
 * net.sf.farrago.namespace.util.FarragoDataWrapperCache}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoPluginCache
    extends FarragoCompoundAllocation
{
    //~ Instance fields --------------------------------------------------------

    private final Map<String, Object> mapMofIdToPlugin;
    private final FarragoObjectCache sharedCache;
    private final FarragoRepos repos;
    private final FarragoPluginClassLoader classLoader;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an empty cache.
     *
     * @param owner FarragoAllocationOwner for this cache, to make sure
     * everything gets discarded eventually
     * @param sharedCache underlying shared cache
     * @param repos FarragoRepos for wrapper initialization
     */
    public FarragoPluginCache(
        FarragoAllocationOwner owner,
        FarragoObjectCache sharedCache,
        FarragoPluginClassLoader classLoader,
        FarragoRepos repos)
    {
        owner.addAllocation(this);
        this.sharedCache = sharedCache;
        this.repos = repos;
        this.classLoader = classLoader;
        this.mapMofIdToPlugin = new HashMap<String, Object>();
    }

    //~ Methods ----------------------------------------------------------------

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
     * @return mapMofIdToPlugin
     */
    public Map<String, Object> getMapMofIdToPlugin()
    {
        return mapMofIdToPlugin;
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
     * @param entry pinned entry from underlying shared cache; key must be
     * plugin object's catalog MofId
     *
     * @return cached plugin object
     */
    protected Object addToPrivateCache(FarragoObjectCache.Entry entry)
    {
        // take ownership of the pinned cache entry
        addAllocation(entry);

        Object obj = entry.getValue();
        mapMofIdToPlugin.put(
            (String) entry.getKey(),
            obj);
        return obj;
    }

    /**
     * Initializes a plugin instance.
     *
     * @param libraryName filename of jar containing plugin implementation
     * @param jarAttributeName name of jar attribute to use to determine class
     * name
     * @param options options with which to initialize plugin
     *
     * @return initialized plugin
     */
    protected FarragoPlugin initializePlugin(
        String libraryName,
        String jarAttributeName,
        Properties options)
    {
        Class pluginClass =
            classLoader.loadClassFromLibraryManifest(
                libraryName,
                jarAttributeName);

        FarragoPlugin plugin;
        try {
            Object obj = classLoader.newPluginInstance(pluginClass);
            assert (obj instanceof FarragoPlugin) : obj.getClass();
            plugin = (FarragoPlugin) obj;
            plugin.initialize(repos, options);
            plugin.setLibraryName(libraryName);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(
                repos.getLocalizedObjectName(libraryName),
                ex);
        }
        return plugin;
    }
}

// End FarragoPluginCache.java
