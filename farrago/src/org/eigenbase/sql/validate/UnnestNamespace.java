/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import org.eigenbase.sql.SqlNode;
import org.eigenbase.reltype.RelDataType;

/**
 * Namespace for UNNEST.
 *
 * @author wael
 * @version $Id$
 * @since Mar 25, 2003
 */
class UnnestNamespace extends AbstractNamespace
{
    private final SqlNode child;
    private final SqlValidatorScope scope;

    UnnestNamespace(
        SqlValidatorImpl validator,
        SqlNode child,
        SqlValidatorScope scope)
    {
        super(validator);
        this.child = child;
        this.scope = scope;
    }

    protected RelDataType validateImpl()
    {
        RelDataType type = validator.deriveType(scope, child);
        if (type.isStruct()) {
            return type;
        }
        return validator.typeFactory.createStructType(
            new RelDataType[]{type},
            new String[]{ validator.deriveAlias(child, 0) });
    }

    public SqlNode getNode()
    {
        return child;
    }
}

// End UnnestNamespace.java

