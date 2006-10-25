/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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
 * FarragoJdbcEngineBooleanParamDef defines a boolean parameter.
 *
 * This class is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcBooleanParamDef
    extends FarragoJdbcParamDef
{

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcBooleanParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return null;
        } else {
            if (x instanceof Boolean) {
                return x;
            } else if (x instanceof Number) {
                Number n = (Number) x;
                return Boolean.valueOf(n.longValue() != 0);
            } else if (x instanceof String) {
                try {
                    return ConversionUtil.toBoolean((String) x);
                } catch (Exception e) {
                    // Convert string to number, return false if zero
                    try {
                        String str = ((String) x).trim();
                        double d = Double.parseDouble(str);
                        return Boolean.valueOf(d != 0);
                    } catch (NumberFormatException ex) {
                        throw newInvalidFormat(x);
                    }
                }
            } else {
                throw newInvalidType(x);
            }
        }
    }
}

// End FarragoJdbcBooleanParamDef.java
