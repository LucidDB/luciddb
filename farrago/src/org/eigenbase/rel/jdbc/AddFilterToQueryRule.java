/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.rel.jdbc;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;


/**
 * <code>AddFilterToQueryRule</code> grafts a {@link FilterRel} onto a {@link
 * JdbcQuery}.
 *
 * <p>This rule only works if the query's select clause is "&#42;". If you start
 * with a {@link FilterRel} on a {@link org.eigenbase.rel.ProjectRel} on a
 * {@link TableAccessRel}, this will not be the case. You can fix it by pushing
 * the filter through the project. (todo: Implement a rule to do this.)</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 26, 2003
 */
class AddFilterToQueryRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    AddFilterToQueryRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(JdbcQuery.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filter = (FilterRel) call.rels[0];
        JdbcQuery oldQuery = (JdbcQuery) call.rels[1];
        if (oldQuery.sql.getSelectList() != null) {
            return;
        }
        JdbcQuery query = oldQuery.clone();
        SqlWriter writer = new SqlPrettyWriter(oldQuery.dialect);
        final RexToSqlTranslator translator = new RexToSqlTranslator();
        final SqlNode sqlCondition =
            translator.translate(
                writer,
                filter.getCondition());
        query.sql.addWhere(sqlCondition);
        call.transformTo(query);
    }
}

// End AddFilterToQueryRule.java
