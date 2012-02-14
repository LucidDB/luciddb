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
package net.sf.farrago.namespace.mdr;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * MedMdrClassExtent represents the relational mapping of a class's extent
 * (stored by MDR) into a queryable set of rows.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrClassExtent
    extends MedAbstractColumnSet
{
    //~ Instance fields --------------------------------------------------------

    final MedMdrNameDirectory directory;
    final RefClass refClass;

    //~ Constructors -----------------------------------------------------------

    MedMdrClassExtent(
        MedMdrNameDirectory directory,
        FarragoTypeFactory typeFactory,
        RefClass refClass,
        String [] foreignName,
        String [] localName,
        RelDataType rowType)
    {
        super(localName, foreignName, rowType, null, null);
        this.directory = directory;
        this.refClass = refClass;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public double getRowCount()
    {
        // REVIEW:  find out how expensive this is!  Once we start applying
        // filters, this is probably bad.
        return refClass.refAllOfType().size();
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        return new MedMdrClassExtentRel(cluster, this, connection);
    }
}

// End MedMdrClassExtent.java
