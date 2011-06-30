/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
 * Namespace whose contents are defined by the type of an {@link
 * org.eigenbase.sql.SqlIdentifier identifier}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class IdentifierNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier id;

    /**
     * The underlying table. Set on validate.
     */
    private SqlValidatorTable table;

    /**
     * List of monotonic expressions. Set on validate.
     */
    private List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an IdentifierNamespace.
     *
     * @param validator Validator
     * @param id Identifier node
     * @param enclosingNode Enclosing node
     */
    IdentifierNamespace(
        SqlValidatorImpl validator,
        SqlIdentifier id,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.id = id;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType validateImpl()
    {
        table = validator.catalogReader.getTable(id.names);
        if (table == null) {
            throw validator.newValidationError(
                id,
                EigenbaseResource.instance().TableNameNotFound.ex(
                    id.toString()));
        }
        if (validator.shouldExpandIdentifiers()) {
            // TODO:  expand qualifiers for column references also
            String [] qualifiedNames = table.getQualifiedName();
            if (qualifiedNames != null) {
                // Assign positions to the components of the fully-qualified
                // identifier, as best we can. We assume that qualification
                // adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
                SqlParserPos [] poses = new SqlParserPos[qualifiedNames.length];
                Arrays.fill(
                    poses,
                    id.getParserPosition());
                int offset = qualifiedNames.length - id.names.length;

                // Test offset in case catalog supports fewer
                // qualifiers than catalog reader.
                if (offset >= 0) {
                    for (int i = 0; i < id.names.length; i++) {
                        poses[i + offset] = id.getComponentParserPosition(i);
                    }
                }
                id.setNames(qualifiedNames, poses);
            }
        }

        // Build a list of monotonic expressions.
        monotonicExprs = new ArrayList<Pair<SqlNode, SqlMonotonicity>>();
        RelDataType rowType = table.getRowType();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            final String fieldName = fields[i].getName();
            final SqlMonotonicity monotonicity =
                table.getMonotonicity(fieldName);
            if (monotonicity != SqlMonotonicity.NotMonotonic) {
                monotonicExprs.add(
                    new Pair<SqlNode, SqlMonotonicity>(
                        new SqlIdentifier(fieldName, SqlParserPos.ZERO),
                        monotonicity));
            }
        }

        // The row type of this namespace is the fields of the table preceded
        // by the set of system fields. As far as the namespace is concerned,
        // all system fields are available.
        //
        // For example, if ROWID were a system field similar to Oracle's ROWID,
        // then all relations would have a ROWID for validation purposes. But if
        // you tried to use the ROWID field of a view based on a join it would
        // say that ROWID is not valid in this context. So, the
        // IdentifierNamespace representing that view would have a ROWID column,
        // but the RelOptTable would not.
        final List<RelDataTypeField> systemFieldList =
            validator.getSystemFields();
        final RelDataType rowTypeIncludingSysFields =
            validator.getTypeFactory().createStructType(
                CompositeList.of(
                    systemFieldList,
                    rowType.getFieldList()));
        setRowType(rowTypeIncludingSysFields, rowType);
        return rowTypeIncludingSysFields;
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

    public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs()
    {
        return monotonicExprs;
    }

    public SqlMonotonicity getMonotonicity(String columnName)
    {
        final SqlValidatorTable table = getTable();
        return table.getMonotonicity(columnName);
    }

}

// End IdentifierNamespace.java
