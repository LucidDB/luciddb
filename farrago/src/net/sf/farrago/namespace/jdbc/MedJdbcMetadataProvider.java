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
package net.sf.farrago.namespace.jdbc;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;

import net.sf.farrago.namespace.impl.*;

import java.util.*;

/**
 * MedJdbcMetadataProvider supplies metadata to the optimizer about JDBC
 * relational expressions.
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcMetadataProvider
    extends ReflectiveRelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    public MedJdbcMetadataProvider()
    {
        List<Class> args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) Boolean.TYPE);
        mapParameterTypes("areColumnsUnique", args);
    }

    public Boolean canRestart(MedJdbcQueryRel rel)
    {
        // We don't support restarting a JDBC query.  We could do it via scroll
        // cursors, but (a) not all drivers implement scroll cursors, and (b)
        // we wouldn't want to unconditionally request a scroll cursor since it
        // adds overhead.  So, get the optimizer to force the buffering inside
        // of Farrago as needed.
        return false;
    }

    public Set<BitSet> getUniqueKeys(MedJdbcQueryRel rel)
    {
        return rel.uniqueKeys;
    }

    public Boolean areColumnsUnique(
        MedJdbcQueryRel rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        Set<BitSet> uniqueColSets = rel.uniqueKeys;
        return MedAbstractColumnMetadata.areColumnsUniqueForKeys(
            uniqueColSets, columns);
    }
}

// End MedJdbcMetadataProvider.java
