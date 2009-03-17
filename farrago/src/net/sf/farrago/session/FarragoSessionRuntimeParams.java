/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.session;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;


/**
 * FarragoSessionRuntimeParams bundles together the large number of constructor
 * parameters needed to instantiate {@link FarragoSessionRuntimeContext}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionRuntimeParams
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Controlling session.
     */
    public FarragoSession session;

    /**
     * If no session is available to provide the plugin classloader, use this
     * classloader instead.
     */
    public FarragoPluginClassLoader pluginClassLoader;

    /**
     * Repos storing object definitions.
     */
    public FarragoRepos repos;

    /**
     * Cache for Fennel tuple streams.
     */
    public FarragoObjectCache codeCache;

    /**
     * Txn-private cache for Fennel tuple streams, or null if streams don't need
     * to be pinned by txn.
     */
    public Map<String, FarragoObjectCache.Entry> txnCodeCache;

    /**
     * Fennel context for transactions.
     */
    public FennelTxnContext fennelTxnContext;

    /**
     * Map of indexes which might be accessed.
     */
    public FarragoSessionIndexMap indexMap;

    /**
     * Array of values bound to dynamic parameters by position.
     */
    public Object [] dynamicParamValues;

    /**
     * Connection-dependent settings.
     */
    public FarragoSessionVariables sessionVariables;

    /**
     * FarragoObjectCache to use for caching FarragoMedDataWrapper instances.
     */
    public FarragoObjectCache sharedDataWrapperCache;

    /**
     * FarragoStreamFactoryProvider to use for registering stream factories.
     */
    public FarragoStreamFactoryProvider streamFactoryProvider;

    /**
     * Whether the context is for a DML statement.
     */
    public boolean isDml;

    /**
     * Map from result set name to row type.
     */
    public Map<String, RelDataType> resultSetTypeMap;

    /**
     * Map from IterCalcRel tag to row type. If a mapping is available, it
     * associates the tag with the type of a table being modified. It would be
     * possible to infer, for example, that result column 1 was being used to
     * insert into a column called "EMPNO".
     */
    public Map<String, RelDataType> iterCalcTypeMap;

    /**
     * An identifier for the executable statement id. This parameter assumes
     * there will be a one to one mapping from statement to context.
     */
    public long stmtId;

    /**
     * Queue on which warnings should be posted, or null if runtime context
     * should create a private queue.
     */
    public FarragoWarningQueue warningQueue;

    /**
     * The current time associated with the statement. If set to zero, this
     * indicates that no current time has yet been set for the statement.
     */
    public long currentTime;
}

// End FarragoSessionRuntimeParams.java
