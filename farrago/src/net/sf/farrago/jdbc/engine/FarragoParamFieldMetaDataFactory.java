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
package net.sf.farrago.jdbc.engine;

import java.util.*;

import net.sf.farrago.jdbc.param.*;
import net.sf.farrago.runtime.*;

import org.eigenbase.reltype.*;


/**
 * Factory class for creating the per-column metadata passed to the client
 * driver for its ParameterMetaData implementation.
 *
 * @author Angel Chang
 * @version $Id$
 * @since March 3, 2006
 */
public class FarragoParamFieldMetaDataFactory
{
    //~ Constructors -----------------------------------------------------------

    /**
     * private constructor to prevent instantiation.
     */
    private FarragoParamFieldMetaDataFactory()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public static FarragoParamFieldMetaData newParamFieldMetaData(
        RelDataType type,
        int mode)
    {
        return FarragoRuntimeJdbcUtil.newParamFieldMetaData(type, mode);
    }

    /**
     * Determines the parameter column meta data from the rowType
     *
     * @param rowType Row type
     *
     * @return Parameter column metadata
     */
    public static FarragoParamFieldMetaData [] newParamMetaData(
        RelDataType rowType,
        int mode)
    {
        FarragoParamFieldMetaData [] metaData;

        List<RelDataTypeField> fieldTypes = rowType.getFieldList();
        int colCnt = fieldTypes.size();
        metaData = new FarragoParamFieldMetaData[colCnt];

        for (int i = 0; i < colCnt; ++i) {
            RelDataTypeField f = fieldTypes.get(i);
            RelDataType relType = f.getType();

            FarragoParamFieldMetaData meta =
                newParamFieldMetaData(relType, mode);
            metaData[i] = meta;
        }
        return metaData;
    }
}

// End FarragoParamFieldMetaDataFactory.java
