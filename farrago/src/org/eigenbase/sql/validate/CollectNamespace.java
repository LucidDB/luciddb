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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;

/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * Examples:
 * <code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code>,
 * <code>SELECT * FROM (TABLE getEmpsInDept(30))</code>.
 *
 * @see CollectScope
 * @author wael
 * @version $Id$
 * @since Mar 25, 2003
 */
public class CollectNamespace extends AbstractNamespace
{
    private final SqlNode child;
    private final SqlValidatorScope scope;

    CollectNamespace(SqlNode child, SqlValidatorScope scope)
    {
        super((SqlValidatorImpl) scope.getValidator());
        this.child = child;
        this.scope = scope;
    }

    protected RelDataType validateImpl()
    {
        // XXX call derive type and make deriveTypeImpl private
        RelDataType type = validator.deriveTypeImpl(scope, child);
        boolean isNullable = type.isNullable();
        final RelDataTypeFactory typeFactory = validator.typeFactory;
        type = typeFactory.createStructType(
            new RelDataType[]{type},
            new String[]{ validator.deriveAlias(child, 0) });
        return typeFactory.createTypeWithNullability(type, isNullable);
    }

    public SqlNode getNode()
    {
        return child;
    }

    public SqlValidatorScope getScope()
    {
        return scope;
    }
}

// End CollectNamespace.java

