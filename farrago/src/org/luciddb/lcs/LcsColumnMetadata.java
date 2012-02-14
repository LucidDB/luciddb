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
package org.luciddb.lcs;

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;


/**
 * LcsColumnMetadata is an implementation of MedAbstractColumnMetadata for
 * LcsRowScanRel
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsColumnMetadata
    extends MedAbstractColumnMetadata
{
    //~ Methods ----------------------------------------------------------------

    protected int mapColumnToField(
        RelNode rel,
        FemAbstractColumn keyCol)
    {
        return ((LcsRowScanRel) rel).getProjectedColumnOrdinal(
            keyCol.getOrdinal());
    }

    protected int mapFieldToColumnOrdinal(RelNode rel, int fieldNo)
    {
        FemAbstractColumn col = mapFieldToColumn(rel, fieldNo);
        if (col == null) {
            return -1;
        }
        return col.getOrdinal();
    }

    protected FemAbstractColumn mapFieldToColumn(RelNode rel, int fieldNo)
    {
        return ((LcsRowScanRel) rel).getColumnForFieldAccess(fieldNo);
    }
}

// End LcsColumnMetadata.java
