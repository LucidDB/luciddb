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

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;


/**
 * FarragoColumnMetadata is a default Farrago implementation of
 * MedAbstractColumnMetadata for table level RelNodes. Note that it does not
 * account for projection or UDTs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoColumnMetadata
    extends MedAbstractColumnMetadata
{
    //~ Methods ----------------------------------------------------------------

    protected int mapColumnToField(
        RelNode rel,
        FemAbstractColumn keyCol)
    {
        if (keyCol.getOrdinal() >= numColumns(rel)) {
            return -1;
        }
        return keyCol.getOrdinal();
    }

    protected int mapFieldToColumnOrdinal(RelNode rel, int fieldNo)
    {
        if ((fieldNo == -1) || (fieldNo >= numColumns(rel))) {
            return -1;
        } else {
            return fieldNo;
        }
    }

    protected FemAbstractColumn mapFieldToColumn(RelNode rel, int fieldNo)
    {
        int colno = mapFieldToColumnOrdinal(rel, fieldNo);
        if ((colno == -1) || (colno >= numColumns(rel))) {
            return null;
        } else {
            return (FemAbstractColumn) ((MedAbstractColumnSet) rel.getTable())
                .getCwmColumnSet().getFeature().get(colno);
        }
    }

    private int numColumns(RelNode rel)
    {
        return ((MedAbstractColumnSet) rel.getTable()).getCwmColumnSet()
            .getFeature().size();
    }
}

// End FarragoColumnMetadata.java
