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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.parser.SqlParserPos;

/**
 * Namespace whose contents are defined by the type of an
 * {@link org.eigenbase.sql.SqlIdentifier identifier}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class IdentifierNamespace extends AbstractNamespace
{
    private final SqlIdentifier id;

    /** The underlying table. Set on validate. */
    private SqlValidatorTable table;

    /** List of monotonic expressions. Set on validate. */
    private SqlNodeList monotonicExprs;

    IdentifierNamespace(SqlValidatorImpl validator, SqlIdentifier id)
    {
        super(validator);
        this.id = id;
    }

    public RelDataType validateImpl()
    {
        table = validator.catalogReader.getTable(id.names);
        if (table == null) {
            throw validator.newValidationError(id,
                EigenbaseResource.instance().TableNameNotFound.ex(
                    id.toString()));
        }
        if (validator.shouldExpandIdentifiers()) {
            // TODO:  expand qualifiers for column references also
            String [] qualifiedNames = table.getQualifiedName();
            if (qualifiedNames != null) {
                id.names = qualifiedNames;
            }
        }
        // Build a list of monotonic expressions.
        monotonicExprs = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = table.getRowType();
        RelDataTypeField[] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            final String fieldName = fields[i].getName();
            if (table.isMonotonic(fieldName)) {
                monotonicExprs.add(
                    new SqlIdentifier(fieldName, SqlParserPos.ZERO));
            }
        }
        // Validation successful.
        return rowType;
    }

    public SqlIdentifier getId()
    {
        return id;
    }

    public SqlNode getNode()
    {
        return id;
    }

    public SqlValidatorTable getTable()
    {
        return table;
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut)
    {
        return null;
    }

    public SqlValidatorNamespace lookupChild(
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut)
    {
        return null;
    }

    public SqlNodeList getMonotonicExprs()
    {
        return monotonicExprs;
    }

    public boolean isMonotonic(String columnName)
    {
        final SqlValidatorTable table = getTable();
        return table.isMonotonic(columnName);
    }
}

// End IdentifierNamespace.java

