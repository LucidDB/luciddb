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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * A scope which delegates all requests to its parent scope. Use this as a base
 * class for defining nested scopes.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public abstract class DelegatingScope
    implements SqlValidatorScope
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Parent scope. This is where to look next to resolve an identifier; it is
     * not always the parent object in the parse tree.
     *
     * <p>This is never null: at the top of the tree, it is an {@link
     * EmptyScope}.
     */
    protected final SqlValidatorScope parent;
    protected final SqlValidatorImpl validator;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>DelegatingScope</code>.
     *
     * @param parent Parent scope
     */
    DelegatingScope(SqlValidatorScope parent)
    {
        super();
        Util.pre(parent != null, "parent != null");
        this.validator = (SqlValidatorImpl) parent.getValidator();
        this.parent = parent;
    }

    //~ Methods ----------------------------------------------------------------

    public void addChild(SqlValidatorNamespace ns, String alias)
    {
        // By default, you cannot add to a scope. Derived classes can
        // override.
        throw new UnsupportedOperationException();
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut)
    {
        return parent.resolve(name, ancestorOut, offsetOut);
    }

    protected void addColumnNames(
        SqlValidatorNamespace ns,
        List<SqlMoniker> colNames)
    {
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
            colNames.add(
                new SqlMonikerImpl(
                    field.getName(),
                    SqlMonikerType.Column));
        }
    }

    public void findAllColumnNames(List<SqlMoniker> result)
    {
        parent.findAllColumnNames(result);
    }

    public void findAliases(List<SqlMoniker> result)
    {
        parent.findAliases(result);
    }

    public String findQualifyingTableName(String columnName, SqlNode ctx)
    {
        return parent.findQualifyingTableName(columnName, ctx);
    }

    public RelDataType resolveColumn(String name, SqlNode ctx)
    {
        return parent.resolveColumn(name, ctx);
    }

    public SqlValidatorScope getOperandScope(SqlCall call)
    {
        if (call instanceof SqlSelect) {
            return validator.getSelectScope((SqlSelect) call);
        }
        return this;
    }

    public SqlValidator getValidator()
    {
        return validator;
    }

    /**
     * Converts an identifier into a fully-qualified identifier. For example,
     * the "empno" in "select empno from emp natural join dept" becomes
     * "emp.empno".
     *
     * <p>If the identifier cannot be resolved, throws. Never returns null.
     */
    public SqlIdentifier fullyQualify(SqlIdentifier identifier)
    {
        if (identifier.isStar()) {
            return identifier;
        }

        String tableName;
        String columnName;

        switch (identifier.names.length) {
        case 1:
            columnName = identifier.names[0];
            tableName =
                findQualifyingTableName(columnName, identifier);

            //todo: do implicit collation here
            final SqlParserPos pos = identifier.getParserPosition();
            SqlIdentifier expanded =
                new SqlIdentifier(
                    new String[] { tableName, columnName },
                    null,
                    pos,
                    new SqlParserPos[] {
                        SqlParserPos.ZERO,
                        pos
                    });
            validator.setOriginal(expanded, identifier);
            return expanded;

        case 2:
            tableName = identifier.names[0];
            final SqlValidatorNamespace fromNs = resolve(tableName, null, null);
            if (fromNs == null) {
                throw validator.newValidationError(
                    identifier.getComponent(0),
                    EigenbaseResource.instance().TableNameNotFound.ex(
                        tableName));
            }
            columnName = identifier.names[1];
            final RelDataType fromRowType = fromNs.getRowType();
            final RelDataType type =
                SqlValidatorUtil.lookupFieldType(fromRowType, columnName);
            if (type != null) {
                return identifier; // it was fine already
            } else {
                throw validator.newValidationError(
                    identifier.getComponent(1),
                    EigenbaseResource.instance().ColumnNotFoundInTable.ex(
                        columnName,
                        tableName));
            }

        default:
            // NOTE jvs 26-May-2004:  lengths greater than 2 are possible
            // for row and structured types
            assert identifier.names.length > 0;
            return identifier;
        }
    }

    public void validateExpr(SqlNode expr)
    {
        // Do not delegate to parent. An expression valid in this scope may not
        // be valid in the parent scope.
    }

    public SqlWindow lookupWindow(String name)
    {
        return parent.lookupWindow(name);
    }

    public SqlMonotonicity getMonotonicity(SqlNode expr)
    {
        return parent.getMonotonicity(expr);
    }

    public SqlNodeList getOrderList()
    {
        return parent.getOrderList();
    }

    /**
     * Returns the parent scope of this <code>DelegatingScope</code>.
     */
    public SqlValidatorScope getParent()
    {
        return parent;
    }
}

// End DelegatingScope.java
