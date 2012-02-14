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
package net.sf.farrago.query;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FarragoExecutableStmtImpl is an abstract base for implementations of
 * FarragoSessionExecutableStmt.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoExecutableStmtImpl
    extends FarragoCompoundAllocation
    implements FarragoSessionExecutableStmt
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer =
        FarragoTrace.getClassTracer(FarragoExecutableStmtImpl.class);

    //~ Instance fields --------------------------------------------------------

    private final boolean isDml;
    private final TableModificationRel.Operation tableModOp;
    private final RelDataType dynamicParamRowType;
    private final TableAccessMap tableAccessMap;

    //~ Constructors -----------------------------------------------------------

    protected FarragoExecutableStmtImpl(
        RelDataType dynamicParamRowType,
        boolean isDml,
        TableModificationRel.Operation tableModOp,
        TableAccessMap tableAccessMap)
    {
        this.isDml = isDml;
        this.tableModOp = tableModOp;
        this.dynamicParamRowType = dynamicParamRowType;
        this.tableAccessMap = tableAccessMap;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public boolean isDml()
    {
        return isDml;
    }

    // implement FarragoSessionExecutableStmt
    public TableModificationRel.Operation getTableModOp()
    {
        return tableModOp;
    }

    // implement FarragoSessionExecutableStmt
    public RelDataType getDynamicParamRowType()
    {
        return dynamicParamRowType;
    }

    // implement FarragoSessionExecutableStmt
    public Set<String> getReferencedObjectIds()
    {
        return Collections.emptySet();
    }

    // implement FarragoSessionExecutableStmt
    public String getReferencedObjectModTime(String mofid)
    {
        return null;
    }

    // implement FarragoSessionExecutableStmt
    public TableAccessMap getTableAccessMap()
    {
        return tableAccessMap;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return Collections.emptyMap();
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getIterCalcTypeMap()
    {
        return Collections.EMPTY_MAP;
    }
}

// End FarragoExecutableStmtImpl.java
