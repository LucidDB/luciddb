/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.jdbc.param;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineBinaryParamDef defines a binary parameter. Only accepts
 * byte-array values. This class is JDK 1.4 compatible.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcBinaryParamDef
    extends FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    private final int maxByteCount;

    //~ Constructors -----------------------------------------------------------

    public FarragoJdbcBinaryParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
        maxByteCount = paramMetaData.precision;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return x;
        }
        if (!(x instanceof byte [])) {
            throw newInvalidType(x);
        }
        final byte [] bytes = (byte []) x;
        if (bytes.length > maxByteCount) {
            throw newValueTooLong(
                ConversionUtil.toStringFromByteArray(bytes, 16));
        }
        return bytes;
    }
}

// End FarragoJdbcBinaryParamDef.java
