/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * ExplicitOperandTypeInferences implements {@link SqlOperandTypeInference} by
 * explicity supplying a type for each parameter.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ExplicitOperandTypeInference
    implements SqlOperandTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType [] paramTypes;

    //~ Constructors -----------------------------------------------------------

    public ExplicitOperandTypeInference(RelDataType [] paramTypes)
    {
        this.paramTypes = paramTypes;
    }

    //~ Methods ----------------------------------------------------------------

    public void inferOperandTypes(
        SqlCallBinding callBinding,
        RelDataType returnType,
        RelDataType [] operandTypes)
    {
        System.arraycopy(paramTypes, 0, operandTypes, 0, paramTypes.length);
    }
}

// End ExplicitOperandTypeInference.java
