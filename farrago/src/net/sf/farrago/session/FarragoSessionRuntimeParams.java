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
     * Embracing statement context
     */
    public FarragoSessionStmtContext stmtContext;

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
