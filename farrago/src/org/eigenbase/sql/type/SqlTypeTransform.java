/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
import org.eigenbase.sql.validate.*;

/**
 * Strategy to transform one type to another. The transformation is
 * dependent on the implemented strategy object and in the general case is
 * a function of the type and the other operands
 *
 * Can not be used by itself. Must be used in an object of type
 * {@link TransformCascade}
 *
 * <p>This class is an example of the
 * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public interface SqlTypeTransform
{
    /**
     * @param typeToTransform The type subject of transformation. The return
     * type is (in the general case) a function of
     * <ul><li>The typeToTransform</li><li>The other operand types</li></ul>
     * {@link SqlReturnTypeInference}  object.
     * @return A new type depending on the operands types
     */
    public RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands,
        RelDataType typeToTransform);
}

// End SqlTypeTransform.java
