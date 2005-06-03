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

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.*;
import org.eigenbase.util.Util;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resource.EigenbaseResource;

import java.util.List;

/**
 * A scope which delegates all requests to its parent scope.
 * Use this as a base class for defining nested scopes.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
abstract class DelegatingScope implements SqlValidatorScope 
{
    /**
     * Parent scope. This is where to look next to resolve an identifier;
     * it is not always the parent object in the parse tree.
     *
     * <p>This is never null: at the top of the tree, it is an
     * {@link EmptyScope}.
     */
    protected final SqlValidatorScope parent;
    protected final SqlValidatorImpl validator;

    DelegatingScope(SqlValidatorScope parent)
    {
        super();
        this.validator = (SqlValidatorImpl) parent.getValidator();
        Util.pre(parent != null, "parent != null");
        this.parent = parent;
    }

    /**
     * Registers a relation in this scope.
     * @param ns Namespace representing the result-columns of the relation
     * @param alias Alias with which to reference the relation, must not
     *   be null
     */
    public void addChild(SqlValidatorNamespace ns, String alias)
    {
        // By default, you cannot add to a scope. Derived classes can
        // override.
        throw new UnsupportedOperationException();
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut)
    {
        return parent.resolve(name, ancestorOut, offsetOut);
    }

    protected void addColumnNames(SqlValidatorNamespace ns,
                                List colNames) {
        final RelDataType rowType;
        try {
            rowType = ns.getRowType();
        } catch (Error e) {
            // namespace is not good - bail out.
            return;
        }

        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            colNames.add(new MonikerImpl(field.getName(), MonikerType.Column));
        }
    }

    protected void addTableNames(SqlValidatorNamespace ns,
                                List tableNames) {
        SqlValidatorTable table = ns.getTable();
        if (table == null) return;
        String [] qnames = table.getQualifiedName();
        if (qnames != null) {
            tableNames.add(new MonikerImpl(qnames, MonikerType.Table));
        }
    }

    public void findAllColumnNames(String parentObjName, List result)
    {
        parent.findAllColumnNames(parentObjName, result);
    }

    public void findAllTableNames(List result)
    {
        parent.findAllTableNames(result);
    }

    public String findQualifyingTableName(String columnName, SqlNode ctx)
    {
        return parent.findQualifyingTableName(columnName, ctx);
    }

    public SqlValidator getValidator()
    {
        return validator;
    }

    /**
     * Converts an identifier into a fully-qualified identifier. For
     * example, the "empno" in "select empno from emp natural join dept"
     * becomes "emp.empno".
     *
     * If the identifier cannot be resolved, throws. Never returns null.
     */
    public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
        if (identifier.isStar()) {
            return identifier;
        }
        switch (identifier.names.length) {
        case 1:
            {
                final String columnName = identifier.names[0];
                final String tableName =
                    findQualifyingTableName(columnName, identifier);

                //todo: do implicit collation here
                return new SqlIdentifier(
                    new String[]{tableName, columnName},
                    SqlParserPos.ZERO);
            }

        case 2:
            {
                final String tableName = identifier.names[0];
                final SqlValidatorNamespace fromNs = resolve(tableName, null, null);
                if (fromNs == null) {
                    throw validator.newValidationError(identifier,
                        EigenbaseResource.instance().newTableNameNotFound(
                            tableName));
                }
                final String columnName = identifier.names[1];
                final RelDataType fromRowType = fromNs.getRowType();
                final RelDataType type =
                    SqlValidatorUtil.lookupField(fromRowType, columnName);
                if (type != null) {
                    return identifier; // it was fine already
                } else {
                    throw EigenbaseResource.instance()
                        .newColumnNotFoundInTable(columnName, tableName);
                }
            }

        default:
            // NOTE jvs 26-May-2004:  lengths greater than 2 are possible
            // for row and structured types
            assert identifier.names.length > 0;
            return identifier;
        }
    }

    public SqlWindow lookupWindow(String name) {
        return parent.lookupWindow(name);
    }

    public boolean isMonotonic(SqlNode expr) {
        return parent.isMonotonic(expr);
    }
}

// End DelegatingScope.java

