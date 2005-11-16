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
package net.sf.farrago.session;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.EigenbaseException;

/**
 * FarragoSessionStmtParamDef represents the definition of a dynamic parameter
 * used within a {@link FarragoSessionStmtContext}.  Instances of 
 * FarragoSessionStmtParamDef are created by a 
 * {@link FarragoSessionStmtParamDefFactory} and are used to validate
 * dynamic parameter values.
 * 
 * @see FarragoSessionStmtContext#setDynamicParam(int, Object)
 * 
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface FarragoSessionStmtParamDef
{
    /**
     * Returns the name of this parameter.
     * 
     * @return the name of this parameter.
     */
    String getParamName();
    
    /**
     * Returns the {@link RelDataType} of this parameter.
     * 
     * @return the {@link RelDataType} of this parameter.
     */
    RelDataType getParamType();
    
    /**
     * Checks the type of a value, and throws an error if it is invalid or
     * cannot be converted to an acceptable type.
     *
     * @param value
     * @return value if valid; an acceptable value if a conversion is available
     * @throws EigenbaseException if value is invalid and cannot be converted
     */
    Object scrubValue(Object value) throws EigenbaseException;
}
