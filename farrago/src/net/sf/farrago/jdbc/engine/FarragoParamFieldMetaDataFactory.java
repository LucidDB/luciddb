/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Portions Copyright (C) 2006-2007 John V. Sichi
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
