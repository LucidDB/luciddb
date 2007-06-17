/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.sql.type.*;


/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * <p>Examples:
 *
 * <ul>
 * <li><code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code>,
 * <li><code>SELECT * FROM (TABLE getEmpsInDept(30))</code>.
 * </ul>
 *
 * <p>NOTE: jhyde, 2006/4/24: These days, this class seems to be used
 * exclusively for the <code>MULTISET</code> construct.
 *
 * @author wael
 * @version $Id$
 * @see CollectScope
 * @since Mar 25, 2003
 */
public class CollectNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall child;
    private final SqlValidatorScope scope;

    //~ Constructors -----------------------------------------------------------

    CollectNamespace(SqlCall child, SqlValidatorScope scope)
    {
        super((SqlValidatorImpl) scope.getValidator());
        this.child = child;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        final RelDataType type =
            child.getOperator().deriveType(validator, scope, child);

        switch (child.getKind().getOrdinal()) {
        case SqlKind.MultisetValueConstructorORDINAL:

            // "MULTISET [<expr>, ...]" needs to be wrapped in a record if
            // <expr> has a scalar type.
            // For example, "MULTISET [8, 9]" has type
            // "RECORD(INTEGER EXPR$0 NOT NULL) NOT NULL MULTISET NOT NULL".
            boolean isNullable = type.isNullable();
            final RelDataType componentType =
                ((MultisetSqlType) type).getComponentType();
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            if (componentType.isStruct()) {
                return type;
            } else {
                final RelDataType structType =
                    typeFactory.createStructType(
                        new RelDataType[] { type },
                        new String[] { validator.deriveAlias(child, 0) });
                final RelDataType multisetType =
                    typeFactory.createMultisetType(structType, -1);
                return typeFactory.createTypeWithNullability(
                    multisetType,
                    isNullable);
            }

        case SqlKind.MultisetQueryConstructorORDINAL:

            // "MULTISET(<query>)" is already a record.
            assert (type instanceof MultisetSqlType)
                && ((MultisetSqlType) type).getComponentType().isStruct() : type;
            return type;
        default:
            throw child.getKind().unexpected();
        }
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
