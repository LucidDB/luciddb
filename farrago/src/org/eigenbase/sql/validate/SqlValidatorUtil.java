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

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Utility methods related to validation.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class SqlValidatorUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a {@link SqlValidatorScope} into a {@link RelOptTable}. This is
     * only possible if the scope represents an identifier, such as "sales.emp".
     * Otherwise, returns null.
     *
     * @param namespace Namespace
     * @param schema Schema
     * @param datasetName Name of sample dataset to substitute, or null to use
     * the regular table
     * @param usedDataset Output parameter which is set to true if a sample
     * dataset is found; may be null
     */
    public static RelOptTable getRelOptTable(
        SqlValidatorNamespace namespace,
        RelOptSchema schema,
        String datasetName,
        boolean [] usedDataset)
    {
        if (namespace instanceof IdentifierNamespace) {
            IdentifierNamespace identifierNamespace =
                (IdentifierNamespace) namespace;
            final String [] names = identifierNamespace.getId().names;
            if ((datasetName != null)
                && (schema instanceof RelOptSchemaWithSampling))
            {
                return ((RelOptSchemaWithSampling) schema).getTableForMember(
                    names,
                    datasetName,
                    usedDataset);
            } else {
                // Schema does not support substitution. Ignore the dataset,
                // if any.
                return schema.getTableForMember(names);
            }
        } else {
            return null;
        }
    }

    /**
     * Looks up a field with a given name and if found returns its type.
     *
     * @param rowType Row type
     * @param columnName Field name
     *
     * @return Field's type, or null if not found
     */
    static RelDataType lookupFieldType(
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

    /**
     * Looks up a field with a given name and if found returns its ordinal.
     *
     * @param rowType Row type
     * @param columnName Field name
     *
     * @return Ordinal of field, or -1 if not found
     */
    public static int lookupField(
        final RelDataType rowType,
        String columnName)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
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
                    throw new Error(
                        type.toString()
                        + " was found to have charset '" + strCharset.name()
                        + "' and a mismatched collation charset '"
                        + colCharset.name() + "'");
                }
            }
        }
    }

    /**
     * Converts an expression "expr" into "expr AS alias".
     */
    public static SqlNode addAlias(
        SqlNode expr,
        String alias)
    {
        final SqlParserPos pos = expr.getParserPosition();
        final SqlIdentifier id = new SqlIdentifier(alias, pos);
        return SqlStdOperatorTable.asOperator.createCall(pos, expr, id);
    }

    /**
     * Derives an alias for a node. If it cannot derive an alias, returns null.
     *
     * <p>This method doesn't try very hard. It doesn't invent mangled aliases,
     * and doesn't even recognize an AS clause. (See {@link #getAlias(SqlNode,
     * int)} for that.) It just takes the last part of an identifier.
     */
    public static String getAlias(SqlNode node)
    {
        if (node instanceof SqlIdentifier) {
            String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];
        } else {
            return null;
        }
    }

    /**
     * Derives an alias for a node, and invents a mangled identifier if it
     * cannot.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Alias: "1 + 2 as foo" yields "foo"
     * <li>Identifier: "foo.bar.baz" yields "baz"
     * <li>Anything else yields "expr$<i>ordinal</i>"
     * </ul>
     *
     * @return An alias, if one can be derived; or a synthetic alias
     * "expr$<i>ordinal</i>" if ordinal >= 0; otherwise null
     */
    public static String getAlias(SqlNode node, int ordinal)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:

            // E.g. "1 + 2 as foo" --> "foo"
            return ((SqlCall) node).getOperands()[1].toString();

        case SqlKind.OverORDINAL:

            // E.g. "bids over w" --> "bids"
            return getAlias(((SqlCall) node).getOperands()[0], ordinal);

        case SqlKind.IdentifierORDINAL:

            // E.g. "foo.bar" --> "bar"
            final String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];

        default:
            if (ordinal < 0) {
                return null;
            } else {
                return SqlUtil.deriveAliasFromOrdinal(ordinal);
            }
        }
    }

    /**
     * Makes a name distinct from other names which have already been used, adds
     * it to the list, and returns it.
     *
     * @param name Suggested name, may not be unique
     * @param nameList Collection of names already used
     *
     * @return Unique name
     */
    public static String uniquify(String name, Collection<String> nameList)
    {
        if (name == null) {
            name = "EXPR$";
        }
        if (nameList.contains(name)) {
            String aliasBase = name;
            for (int j = 0;; j++) {
                name = aliasBase + j;
                if (!nameList.contains(name)) {
                    break;
                }
            }
        }
        nameList.add(name);
        return name;
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
            typeFactory,
            SqlValidator.Compatible.Default);
    }

    /**
     * Makes sure that the names in a list are unique.
     */
    public static void uniquify(List<String> nameList)
    {
        List<String> usedList = new ArrayList<String>();
        for (int i = 0; i < nameList.size(); i++) {
            String name = nameList.get(i);
            String uniqueName = uniquify(name, usedList);
            if (!uniqueName.equals(name)) {
                nameList.set(i, uniqueName);
            }
        }
    }

    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
     * namespace. The returned namespace may represent a schema, table, column,
     * etc.
     *
     * @pre names.size() > 0
     * @post return != null
     */
    public static SqlValidatorNamespace lookup(
        SqlValidatorScope scope,
        List<String> names)
    {
        Util.pre(names.size() > 0, "names.size() > 0");
        SqlValidatorNamespace namespace = null;
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (i == 0) {
                namespace = scope.resolve(name, null, null);
            } else {
                namespace = namespace.lookupChild(name, null, null);
            }
        }
        Util.permAssert(namespace != null, "post: namespace != null");
        return namespace;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over an expression, copying every node, and fully-qualifying every
     * identifier.
     */
    public static class DeepCopier
        extends SqlScopedShuttle
    {
        DeepCopier(SqlValidatorScope scope)
        {
            super(scope);
        }

        public SqlNode visit(SqlNodeList list)
        {
            SqlNodeList copy = new SqlNodeList(list.getParserPosition());
            for (SqlNode node : list) {
                copy.add(node.accept(this));
            }
            return copy;
        }

        // Override to copy all arguments regardless of whether visitor changes
        // them.
        protected SqlNode visitScoped(SqlCall call)
        {
            ArgHandler<SqlNode> argHandler =
                new CallCopyingArgHandler(call, true);
            call.getOperator().acceptCall(this, call, false, argHandler);
            return argHandler.result();
        }

        public SqlNode visit(SqlLiteral literal)
        {
            return (SqlNode) literal.clone();
        }

        public SqlNode visit(SqlIdentifier id)
        {
            return getScope().fullyQualify(id);
        }

        public SqlNode visit(SqlDataTypeSpec type)
        {
            return (SqlNode) type.clone();
        }

        public SqlNode visit(SqlDynamicParam param)
        {
            return (SqlNode) param.clone();
        }

        public SqlNode visit(SqlIntervalQualifier intervalQualifier)
        {
            return (SqlNode) intervalQualifier.clone();
        }
    }
}

// End SqlValidatorUtil.java
