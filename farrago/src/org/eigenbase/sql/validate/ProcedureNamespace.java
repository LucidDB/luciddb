/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
 * Namespace whose contents are defined by the result of a call to a
 * user-defined procedure.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ProcedureNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlValidatorScope scope;

    private final SqlCall call;

    //~ Constructors -----------------------------------------------------------

    ProcedureNamespace(
        SqlValidatorImpl validator,
        SqlValidatorScope scope,
        SqlCall call,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.scope = scope;
        this.call = call;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType validateImpl()
    {
        validator.inferUnknownTypes(validator.unknownType, scope, call);
        return validator.deriveTypeImpl(scope, call);
    }

    public SqlNode getNode()
    {
        return call;
    }
}

// End ProcedureNamespace.java
