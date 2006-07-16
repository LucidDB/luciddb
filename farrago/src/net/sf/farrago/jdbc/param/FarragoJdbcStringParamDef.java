/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

/**
 * FarragoJdbcEngineStringParamDef defines a string parameter. Values which 
 * are not strings are converted into strings. Strings are not padded, even
 * for CHAR columns.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcStringParamDef extends FarragoJdbcParamDef
{
    private final int maxCharCount;

    public FarragoJdbcStringParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
        maxCharCount = paramMetaData.precision;
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return x;
        }
        if (x instanceof String) {
            return x;
        }
        if (x instanceof byte[]) {
            // Don't allow binary to placed in string
            throw newInvalidType(x);
        }
        // REVIEW jvs 7-Oct-2004: the default toString() implementation for
        // Float/Double/Date/Time/Timestamp/byte[] may not be correct here.
        final String s = x.toString();
        if (s.length() > maxCharCount) {
            throw newValueTooLong(s);
        }
        return s;
    }
}