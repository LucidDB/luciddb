/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.util.Util;

/**
 * Strategy interface to infer the type of an operator call from the type of the
 * operands.
 *
 * <p>This interface is an example of the
 * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
 * This makes sense because many operators have similar, straightforward
 * strategies, such as to take the type of the first operand.</p>
 *
 * @author Wael Chatila
 * @since Sept 8, 2004
 * @version $Id$
 */
public interface ReturnTypeInference
{
    RelDataType getType(SqlValidator validator,
                        SqlValidator.Scope scope,
                        RelDataTypeFactory typeFactory,
                        CallOperands callOperands);
}

// End ReturnTypeInference.java

