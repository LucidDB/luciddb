/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import net.sf.farrago.resource.FarragoResource;

import org.eigenbase.reltype.RelDataType;

/**
 * FarragoJdbcEngineStringParamDef defines a string parameter. Values which 
 * are not strings are converted into strings. Strings are not padded, even
 * for CHAR columns.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineStringParamDef extends FarragoJdbcEngineParamDef
{
    private final int maxCharCount;

    public FarragoJdbcEngineStringParamDef(
        String paramName,
        RelDataType type)
    {
        super(paramName, type);
        maxCharCount = type.getPrecision();
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            return x;
        }
        if (x instanceof String) {
            return x;
        }
        // REVIEW jvs 7-Oct-2004: the default toString() implementation for
        // Float/Double/Date/Time/Timestamp/byte[] may not be correct here.
        final String s = x.toString();
        if (s.length() > maxCharCount) {
            throw FarragoResource.instance().ParameterValueTooLong.ex(
                s,
                type.toString());
        }
        return s;
    }
}