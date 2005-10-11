/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.namespace.util;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;

import net.sf.farrago.fem.med.*;

import org.eigenbase.reltype.*;


/**
 * FarragoDataWrapperCache serves as a private cache of
 * FarragoMedDataWrapper and FarragoMedDataServer instances.  It requires an
 * underlying shared FarragoObjectCache.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDataWrapperCache extends FarragoPluginCache
{
    //~ Instance fields -------------------------------------------------------

    private FennelDbHandle fennelDbHandle;

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
     *
     * @param fennelDbHandle FennelDbHandle for wrapper initialization
     */
    public FarragoDataWrapperCache(
        FarragoAllocationOwner owner,
        FarragoObjectCache sharedCache,
        FarragoPluginClassLoader classLoader,
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle)
    {
        super(owner, sharedCache, classLoader, repos);
        this.fennelDbHandle = fennelDbHandle;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Loads a data wrapper, or uses a cached instance.  If the requested
     * wrapper is already available in this private or in the shared cache, it
     * can be used directly.  Otherwise, the wrapper is loaded, initialized,
     * and cached at both levels.
     *
     * @param mofId key by which the wrapper can be uniquely identified
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
        FarragoMedDataWrapper wrapper =
            (FarragoMedDataWrapper) searchPrivateCache(mofId);
        if (wrapper != null) {
            // already privately cached
            return wrapper;
        }

        // otherwise, try shared cache (pin exclusive since wrappers
        // may only be usable by one thread at a time)
        WrapperFactory factory =
            new WrapperFactory(mofId, libraryName, options);
        FarragoObjectCache.Entry entry =
            getSharedCache().pin(mofId, factory, true);

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
        FarragoMedDataServer server =
            (FarragoMedDataServer) searchPrivateCache(mofId);
        if (server != null) {
            // already privately cached
            return server;
        }

        // otherwise, try shared cache
        ServerFactory factory = new ServerFactory(mofId, dataWrapper, options);

        // Use pin exclusive if data wrapper does not support server sharing
        boolean exclusive = !dataWrapper.supportsServerSharing();
        FarragoObjectCache.Entry entry =
            getSharedCache().pin(mofId, factory, exclusive);

        server = (FarragoMedDataServer) addToPrivateCache(entry);
        return server;
    }

    /**
     * Loads a data wrapper based on its catalog definition.
     *
     * @param femWrapper catalog definition for data wrapper to load
     *
     * @return loaded data wrapper
     */
    public FarragoMedDataWrapper loadWrapperFromCatalog(
        FemDataWrapper femWrapper)
    {
        Properties props = getStorageOptionsAsProperties(femWrapper);
        return loadWrapper(
            femWrapper.refMofId(),
            femWrapper.getLibraryFile(),
            props);
    }
    
    /**
     * Loads a data server based on its catalog definition.
     *
     * @param femServer catalog definition for data server to load
     *
     * @return loaded data server
     */
    public FarragoMedDataServer loadServerFromCatalog(FemDataServer femServer)
    {
        Properties props =
            getStorageOptionsAsProperties(femServer);

        String val;
        val = femServer.getType();
        if (val != null) {
            props.setProperty(FarragoMedDataServer.PROP_SERVER_TYPE, val);
        }
        val = femServer.getVersion();
        if (val != null) {
            props.setProperty(FarragoMedDataServer.PROP_SERVER_VERSION, val);
        }
        val = femServer.getName();
        if (val != null) {
            props.setProperty(FarragoMedDataServer.PROP_SERVER_NAME, val);
        }

        FemDataWrapper femDataWrapper = femServer.getWrapper();

        FarragoMedDataWrapper dataWrapper =
            loadWrapperFromCatalog(femDataWrapper);

        return loadServer(
            femServer.refMofId(),
            dataWrapper,
            props);
    }

    /**
     * Loads a FarragoMedColumnSet from its catalog definition.
     *
     * @param baseColumnSet column set definition
     *
     * @param typeFactory factory for column types
     *
     * @return loaded column set
     */
    public FarragoMedColumnSet loadColumnSetFromCatalog(
        FemBaseColumnSet baseColumnSet,
        FarragoTypeFactory typeFactory)
    {
        FemDataServer femServer = baseColumnSet.getServer();
        
        String [] qualifiedName =
            new String [] {
                baseColumnSet.getNamespace().getNamespace().getName(),
                baseColumnSet.getNamespace().getName(),
                baseColumnSet.getName()
            };

        Properties props =
            getStorageOptionsAsProperties(baseColumnSet);

        Map columnPropMap = new HashMap();

        RelDataType rowType =
            typeFactory.createStructTypeFromClassifier(baseColumnSet);

        Iterator iter = baseColumnSet.getFeature().iterator();
        while (iter.hasNext()) {
            FemStoredColumn column = (FemStoredColumn) iter.next();
            columnPropMap.put(
                column.getName(),
                getStorageOptionsAsProperties(column));
        }

        FarragoMedDataServer medServer = loadServerFromCatalog(femServer);

        FarragoMedColumnSet loadedColumnSet;
        try {
            loadedColumnSet =
                medServer.newColumnSet(qualifiedName, props, typeFactory,
                    rowType, columnPropMap);
        } catch (Throwable ex) {
            throw FarragoResource.instance().ForeignTableAccessFailed.ex(
                typeFactory.getRepos().getLocalizedObjectName(
                    baseColumnSet, null),
                ex);
        }

        if (baseColumnSet.getAllowedAccess() == null) {
            // Allowed access not specified, use allowed access from loadedColumnSet
            baseColumnSet.setAllowedAccess(
                loadedColumnSet.getAllowedAccess().toString());
        } else {
            // TODO: Check allowed access are the same
        }

        if (rowType != null) {
            assert (rowType.equals(loadedColumnSet.getRowType()));
        }

        return loadedColumnSet;
    }
    
    private Properties getStorageOptionsAsProperties(
        FemElementWithStorageOptions element)
    {
        Properties props = new Properties();

        // TODO:  validate no duplicates
        Iterator iter = element.getStorageOptions().iterator();
        while (iter.hasNext()) {
            FemStorageOption option = (FemStorageOption) iter.next();
            props.setProperty(
                option.getName(),
                option.getValue());
        }
        return props;
    }

    //~ Inner Classes ---------------------------------------------------------

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
            Object key,
            FarragoObjectCache.UninitializedEntry entry)
        {
            assert (mofId == key);

            FarragoMedDataWrapper wrapper =
                (FarragoMedDataWrapper) initializePlugin(libraryName,
                    "DataWrapperClassName", options);

            // TODO:  some kind of resource usage estimations for wrappers
            entry.initialize(wrapper, 1);
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
            Object key,
            FarragoObjectCache.UninitializedEntry entry)
        {
            assert (mofId == key);

            FarragoMedDataServer server;
            try {
                server = dataWrapper.newServer(mofId, options);
            } catch (Throwable ex) {
                throw FarragoResource.instance().DataServerInitFailed.ex(ex);
            }

            if (!dataWrapper.isForeign()) {
                FarragoMedLocalDataServer localServer =
                    (FarragoMedLocalDataServer) server;
                localServer.setFennelDbHandle(fennelDbHandle);
            }

            // TODO:  some kind of resource usage estimations for wrappers
            entry.initialize(server, 1);
        }
    }
}


// End FarragoDataWrapperCache.java
