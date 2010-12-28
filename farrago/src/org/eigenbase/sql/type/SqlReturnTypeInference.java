/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
 * Strategy interface to infer the type of an operator call from the type of the
 * operands.
 *
 * <p>This interface is an example of the {@link
 * org.eigenbase.util.Glossary#StrategyPattern strategy pattern}. This makes
 * sense because many operators have similar, straightforward strategies, such
 * as to take the type of the first operand.</p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Sept 8, 2004
 */
public interface SqlReturnTypeInference
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Infers the return type of a call to an {@link SqlOperator}.
     *
     * @param opBinding description of operator binding
     *
     * @return inferred type
     */
    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding);
}

// End SqlReturnTypeInference.java
