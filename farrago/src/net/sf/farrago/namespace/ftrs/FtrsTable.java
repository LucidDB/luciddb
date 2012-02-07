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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * An implementation of RelOptTable for accessing data stored in FTRS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTable
    extends MedAbstractColumnSet
{
    //~ Instance fields --------------------------------------------------------

    private FtrsIndexGuide indexGuide;

    //~ Constructors -----------------------------------------------------------

    FtrsTable(
        String [] localName,
        RelDataType rowType,
        Properties tableProps,
        Map<String, Properties> columnPropMap)
    {
        super(localName, null, rowType, tableProps, columnPropMap);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        return new FtrsIndexScanRel(
            cluster,
            this,
            FarragoCatalogUtil.getClusteredIndex(
                getPreparingStmt().getRepos(),
                getCwmColumnSet()),
            connection,
            null,
            false);
    }

    public FtrsIndexGuide getIndexGuide()
    {
        // have to defer initialization because not all information
        // is available at construction time
        if (indexGuide == null) {
            indexGuide =
                new FtrsIndexGuide(
                    getPreparingStmt().getFarragoTypeFactory(),
                    getCwmColumnSet());
        }
        return indexGuide;
    }
}

// End FtrsTable.java
