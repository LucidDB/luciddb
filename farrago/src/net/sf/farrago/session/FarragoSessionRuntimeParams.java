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
package net.sf.farrago.session;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;

/**
 * FarragoSessionRuntimeParams bundles together the large number
 * of constructor parameters needed to instantiate
 * {@link FarragoSessionRuntimeContext}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionRuntimeParams
{
    //~ Instance fields -------------------------------------------------------

    /**
     * Controlling session.
     */
    public FarragoSession session;
    
    /**
     * Repos storing object definitions.
     */
    public FarragoRepos repos;

    /**
     * Cache for Fennel tuple streams.
     */
    public FarragoObjectCache codeCache;

    /**
     * Txn-private cache for Fennel tuple streams, or null if streams don't
     * need to be pinned by txn.
     */
    public Map txnCodeCache;

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
}


// End FarragoSessionRuntimeParams.java
