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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Namespace representing the type of a dynamic parameter.
 *
 * @author jhyde
 * @version $Id$
 * @see ParameterScope
 * @since Mar 25, 2003
 */
class ParameterNamespace
    extends AbstractNamespace
{

    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    public ParameterNamespace(SqlValidatorImpl validator, RelDataType type)
    {
        super(validator);
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return null;
    }

    public RelDataType validateImpl()
    {
        return type;
    }

    public RelDataType getRowType()
    {
        return type;
    }
}

// End ParameterNamespace.java
