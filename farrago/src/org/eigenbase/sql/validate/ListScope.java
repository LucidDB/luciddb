/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
import org.eigenbase.util.*;


/**
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public abstract class ListScope
    extends DelegatingScope
{
    //~ Instance fields --------------------------------------------------------

    /**
     * List of child {@link SqlValidatorNamespace} objects.
     */
    protected final List<SqlValidatorNamespace> children =
        new ArrayList<SqlValidatorNamespace>();

    /**
     * Aliases of the {@link SqlValidatorNamespace} objects.
     */
    protected final List<String> childrenNames = new ArrayList<String>();

    //~ Constructors -----------------------------------------------------------

    public ListScope(SqlValidatorScope parent)
    {
        super(parent);
    }

    //~ Methods ----------------------------------------------------------------

    public void addChild(SqlValidatorNamespace ns, String alias)
    {
        Util.pre(alias != null, "alias != null");
        children.add(ns);
        childrenNames.add(alias);
    }

    /**
     * Returns an immutable list of child namespaces.
     *
     * @return list of child namespaces
     */
    public List<SqlValidatorNamespace> getChildren()
    {
        return Collections.unmodifiableList(children);
    }

    protected SqlValidatorNamespace getChild(String alias)
    {
        if (alias == null) {
            if (children.size() != 1) {
                throw Util.newInternal(
                    "no alias specified, but more than one table in from list");
            }
            return children.get(0);
        } else {
            for (int i = 0; i < children.size(); i++) {
                if (childrenNames.get(i).equals(alias)) {
                    return children.get(i);
                }
            }
            return null;
        }
    }

    public void findAllColumnNames(List<SqlMoniker> result)
    {
        for (SqlValidatorNamespace ns : children) {
            addColumnNames(ns, result);
        }
        parent.findAllColumnNames(result);
    }

    public void findAliases(List<SqlMoniker> result)
    {
        for (String childrenName : childrenNames) {
            result.add(new SqlMonikerImpl(childrenName, SqlMonikerType.Table));
        }
        parent.findAliases(result);
    }

    public String findQualifyingTableName(
        final String columnName,
        SqlNode ctx)
    {
        int count = 0;
        String tableName = null;
        for (int i = 0; i < children.size(); i++) {
            SqlValidatorNamespace ns = children.get(i);
            final RelDataType rowType = ns.getRowType();
            if (SqlValidatorUtil.lookupField(rowType, columnName) >= 0) {
                tableName = childrenNames.get(i);
                count++;
            }
        }
        switch (count) {
        case 0:
            return parent.findQualifyingTableName(columnName, ctx);
        case 1:
            return tableName;
        default:
            throw validator.newValidationError(
                ctx,
                EigenbaseResource.instance().ColumnAmbiguous.ex(columnName));
        }
    }

    public SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut)
    {
        // First resolve by looking through the child namespaces.
        final int i = childrenNames.indexOf(name);
        if (i >= 0) {
            if (ancestorOut != null) {
                ancestorOut[0] = this;
            }
            if (offsetOut != null) {
                offsetOut[0] = i;
            }
            return children.get(i);
        }

        // Then call the base class method, which will delegate to the
        // parent scope.
        return parent.resolve(name, ancestorOut, offsetOut);
    }

    public RelDataType resolveColumn(String columnName, SqlNode ctx)
    {
        int found = 0;
        RelDataType theType = null;
        for (SqlValidatorNamespace childNs : children) {
            final RelDataType childRowType = childNs.getRowType();
            final RelDataType type =
                SqlValidatorUtil.lookupFieldType(childRowType, columnName);
            if (type != null) {
                found++;
                theType = type;
            }
        }
        if (found == 0) {
            return null;
        } else if (found > 1) {
            throw validator.newValidationError(
                ctx,
                EigenbaseResource.instance().ColumnAmbiguous.ex(columnName));
        } else {
            return theType;
        }
    }
}

// End ListScope.java
