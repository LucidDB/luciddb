/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * A scope which contains nothing besides a few parameters. Like {@link
 * EmptyScope} (which is its base class), it has no parent scope.
 *
 * @author jhyde
 * @version $Id$
 * @see ParameterNamespace
 * @since Mar 25, 2003
 */
public class ParameterScope
    extends EmptyScope
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Map from the simple names of the parameters to types of the parameters
     * ({@link RelDataType}).
     */
    private final Map<String, RelDataType> nameToTypeMap;

    //~ Constructors -----------------------------------------------------------

    public ParameterScope(
        SqlValidatorImpl validator,
        Map<String, RelDataType> nameToTypeMap)
    {
        super(validator);
        this.nameToTypeMap = nameToTypeMap;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlIdentifier fullyQualify(SqlIdentifier identifier)
    {
        return identifier;
    }

    public SqlValidatorScope getOperandScope(SqlCall call)
    {
        return this;
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut)
    {
        final RelDataType type = nameToTypeMap.get(name);
        return new ParameterNamespace(validator, type);
    }
}

// End ParameterScope.java
