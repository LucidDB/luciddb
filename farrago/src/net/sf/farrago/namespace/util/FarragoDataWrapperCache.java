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

package net.sf.farrago.namespace.util;

import net.sf.farrago.namespace.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.catalog.*;

import net.sf.saffron.util.*;

import java.net.*;
import java.util.*;
import java.util.jar.*;

// TODO:  generalize to support any kind of plugin cache

/**
 * FarragoDataWrapperCache serves as a private cache of
 * FarragoMedDataWrapper and FarragoMedDataServer instances.  It requires an
 * underlying shared FarragoObjectCache.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDataWrapperCache extends FarragoCompoundAllocation
{
    private FarragoObjectCache sharedCache;

    private Map mapMofIdToWrapper;

    private FarragoCatalog catalog;
    
    /**
     * Creates an empty cache.
     *
     * @param owner FarragoAllocationOwner for this cache, to make sure
     * everything gets discarded eventually
     *
     * @param sharedCache underlying shared cache
     *
     * @param catalog FarragoCatalog for wrapper initialization
     */
    public FarragoDataWrapperCache(
        FarragoAllocationOwner owner,
        FarragoObjectCache sharedCache,
        FarragoCatalog catalog)
    {
        owner.addAllocation(this);
        this.sharedCache = sharedCache;
        this.catalog = catalog;
        mapMofIdToWrapper = new HashMap();
    }

    private Object searchPrivateCache(String mofId)
    {
        return mapMofIdToWrapper.get(mofId);
    }

    private Object addToPrivateCache(
        FarragoObjectCache.Entry entry)
    {
        // take ownership of the pinned cache entry
        addAllocation(entry);

        Object obj = entry.getValue();
        mapMofIdToWrapper.put(entry.getKey(),obj);
        return obj;
    }

    /**
     * Loads a data wrapper, or uses a cached instance.  If the requested
     * wrapper is already available in this private or in the shared cache, it
     * can be used directly.  Otherwise, the wrapper is loaded, initialized,
     * and cached at both levels.
     *
     * @param mofId key by which the wrapper can uniquely identified
     * (Farrago uses the MofId of the catalog object representing the wrapper)
     *
     * @param libraryName name of the library containing the wrapper
     * implementation
     *
     * @param options wrapper-specific options
     *
     * @return loaded wrapper
     */
    public FarragoMedDataWrapper loadWrapper(
        String mofId,
        String libraryName,
        Properties options)
    {
        // TODO:  for cache hits, assert that libraryName and properties match?

        // first try private cache
        FarragoMedDataWrapper wrapper = (FarragoMedDataWrapper)
            searchPrivateCache(mofId);
        if (wrapper != null) {
            // already privately cached
            return wrapper;
        }

        // otherwise, try shared cache (pin exclusive since wrappers
        // may only be usable by one thread at a time)
        WrapperFactory factory = new WrapperFactory(mofId,libraryName,options);
        FarragoObjectCache.Entry entry = sharedCache.pin(
            mofId,
            factory,
            true);

        wrapper = (FarragoMedDataWrapper) addToPrivateCache(entry);
        return wrapper;
    }

    /**
     * Loads a data server, or uses a cached instance.
     *
     * @param mofId key by which the server can be uniquely identified
     * (Farrago uses the MofId of the catalog object representing the server)
     *
     * @param dataWrapper FarragoMedDataWrapper which
     * provides access to the server
     *
     * @param options server-specific options
     *
     * @return loaded server
     */
    public FarragoMedDataServer loadServer(
        String mofId,
        FarragoMedDataWrapper dataWrapper,
        Properties options)
    {
        // TODO:  for cache hits, assert that properties match?

        // first try private cache
        FarragoMedDataServer server = (FarragoMedDataServer)
            searchPrivateCache(mofId);
        if (server != null) {
            // already privately cached
            return server;
        }
        
        // otherwise, try shared cache
        ServerFactory factory = new ServerFactory(mofId,dataWrapper,options);
        FarragoObjectCache.Entry entry = sharedCache.pin(
            mofId,
            factory,
            true);
        
        server = (FarragoMedDataServer) addToPrivateCache(entry);
        return server;
    }

    private Class loadPluginClass(String libraryName)
    {
        try {
            JarFile jar = new JarFile(libraryName);
            Manifest manifest = jar.getManifest();
            String className =
                manifest.getMainAttributes().getValue("DataWrapperClassName");
            URLClassLoader classLoader = new URLClassLoader(
                new URL [] {new URL("file:" + libraryName)});
            return classLoader.loadClass(className);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newDataWrapperJarLoadFailed(
                libraryName,
                ex);
        }
    }
    
    private class WrapperFactory
        implements FarragoObjectCache.CachedObjectFactory 
    {
        private String mofId;
        private String libraryName;
        private Properties options;
        
        WrapperFactory(
            String mofId,
            String libraryName,
            Properties options)
        {
            this.mofId = mofId;
            this.libraryName = libraryName;
            this.options = options;
        }

        // implement CachedObjectFactory
        public void initializeEntry(
            Object key,FarragoObjectCache.UninitializedEntry entry)
        {
            assert(mofId == key);

            Class pluginClass = loadPluginClass(libraryName);
            
            FarragoMedDataWrapper wrapper;
            try {
                wrapper = (FarragoMedDataWrapper)
                    pluginClass.newInstance();
                wrapper.initialize(catalog,options);
            } catch (Throwable ex) {
                throw FarragoResource.instance().newDataWrapperInitFailed(
                    libraryName,
                    ex);
            }
            
            // TODO:  some kind of resource usage estimations for wrappers
            entry.initialize(wrapper,1);
        }
    }

    private class ServerFactory
        implements FarragoObjectCache.CachedObjectFactory 
    {
        private String mofId;
        private FarragoMedDataWrapper dataWrapper;
        private Properties options;

        ServerFactory(
            String mofId,
            FarragoMedDataWrapper dataWrapper,
            Properties options)
        {
            this.mofId = mofId;
            this.dataWrapper = dataWrapper;
            this.options = options;
        }

        // implement CachedObjectFactory
        public void initializeEntry(
            Object key,FarragoObjectCache.UninitializedEntry entry)
        {
            assert(mofId == key);

            FarragoMedDataServer server;
            try {
                server = dataWrapper.newServer(
                    mofId,
                    options);
            } catch (Throwable ex) {
                throw FarragoResource.instance().newDataServerInitFailed(ex);
            }
            
            // TODO:  some kind of resource usage estimations for wrappers
            entry.initialize(server,1);
        }
    }
}

// End FarragoDataWrapperCache.java
