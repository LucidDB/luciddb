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
package net.sf.farrago.namespace.mql;

import java.math.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMqlColumnSet provides an implementation of the {@link
 * FarragoMedColumnSet} interface for MQL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMqlColumnSet
    extends MedAbstractColumnSet
{
    //~ Instance fields --------------------------------------------------------

    final MedMqlDataServer server;
    final String metawebType;
    final String udxSpecificName;

    //~ Constructors -----------------------------------------------------------

    MedMqlColumnSet(
        MedMqlDataServer server,
        String [] localName,
        RelDataType rowType,
        String metawebType,
        String udxSpecificName)
    {
        super(localName, null, rowType, null, null);
        this.server = server;
        this.udxSpecificName = udxSpecificName;
        this.metawebType = metawebType;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        return new MedMqlTableRel(
            cluster, this, connection);
    }
}

// End MedMqlColumnSet.java
