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

import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeUtil;

import java.nio.charset.Charset;

/**
 * Utility methods related to validation.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class SqlValidatorUtil
{
    /**
     * Converts a {@link SqlValidatorScope} into a
     * {@link RelOptTable}. This is only possible if
     * the scope represents an identifier, such as "sales.emp". Otherwise,
     * returns null.
     */
    public static RelOptTable getRelOptTable(
        SqlValidatorNamespace namespace,
        RelOptSchema schema)
    {
        if (namespace instanceof IdentifierNamespace) {
            IdentifierNamespace identifierNamespace =
                (IdentifierNamespace) namespace;
            final String [] names = identifierNamespace.getId().names;
            return schema.getTableForMember(names);
        } else {
            return null;
        }
    }

    static RelDataType lookupField(
        final RelDataType rowType,
        String columnName)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(columnName)) {
                return field.getType();
            }
        }
        return null;
    }

    public static void checkCharsetAndCollateConsistentIfCharType(
        RelDataType type)
    {
        //(every charset must have a default collation)
        if (SqlTypeUtil.inCharFamily(type)) {
            Charset strCharset = type.getCharset();
            Charset colCharset = type.getCollation().getCharset();
            assert (null != strCharset);
            assert (null != colCharset);
            if (!strCharset.equals(colCharset)) {
                if (false) {
                    // todo: enable this checking when we have a charset to
                    //   collation mapping
                    throw new Error(type.toString() +
                        " was found to have charset '" + strCharset.name() +
                        "' and a mismatched collation charset '" +
                        colCharset.name() + "'");
                }
            }
        }
    }

    /**
     * Converts an expression "expr" into "expr AS alias".
     */
    static SqlNode addAlias(
        SqlNode expr,
        String alias)
    {
        final SqlIdentifier id = new SqlIdentifier(alias, expr.getParserPosition());
        return SqlStdOperatorTable.asOperator.createCall(
            expr, id, SqlParserPos.ZERO);
    }

    /**
     * Derives an alias for a node. If it cannot derive an alias, returns null.
     *
     * <p>This method doesn't try very hard. It doesn't invent mangled aliases,
     * and doesn't even recognize an AS clause. There are other methods for
     * that. It just takes the last part of an identifier.
     */
    public static String getAlias(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            String[] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];
        } else {
            return null;
        }
    }

    static SqlNodeList deepCopy(SqlNodeList list) {
        SqlNodeList copy = new SqlNodeList(list.getParserPosition());
        for (int i = 0; i < list.size(); i++) {
            SqlNode node = list.get(i);
            copy.add(deepCopy(node));
        }
        return copy;
    }

    static SqlNode deepCopy(SqlNode node) {
        if (node instanceof SqlCall) {
            return deepCopy((SqlCall) node);
        } else {
            return (SqlNode) node.clone();
        }
    }

    static SqlCall deepCopy(SqlCall call) {
        SqlCall copy = (SqlCall) call.clone();
        for (int i = 0; i < copy.operands.length; i++) {
            copy.operands[i] = deepCopy(copy.operands[i]);
        }
        return copy;
    }

    /**
     * Factory method for {@link SqlValidator}.
     */
    public static SqlValidatorWithHints newValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory)
    {
        return new SqlValidatorImpl(
            opTab,
            catalogReader,
            typeFactory);
    }
}

// End SqlValidatorUtil.java
