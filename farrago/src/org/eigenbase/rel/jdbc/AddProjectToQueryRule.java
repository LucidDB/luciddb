/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.parser.SqlParserPos;


/**
 * A <code>AddProjectToQueryRule</code> grafts a {@link ProjectRel} onto a
 * {@link JdbcQuery}. This rule does not apply if the query already has a
 * select list (other than the default, null, which means '&#42;'). todo:
 * Write a rule to fuse two {@link ProjectRel}s together.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Aug 7, 2002
 */
class AddProjectToQueryRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    AddProjectToQueryRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(JdbcQuery.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        JdbcQuery oldQuery = (JdbcQuery) call.rels[1];
        if (oldQuery.sql.getSelectList() != null) {
            return; // don't try to fuse select list onto select list
        }
        JdbcQuery query =
            new JdbcQuery(
                oldQuery.getCluster(),
                project.getRowType(),
                oldQuery.connection,
                oldQuery.dialect,
                (SqlSelect) oldQuery.sql.clone(),
                oldQuery.dataSource);
        SqlWriter writer = new SqlWriter(query.dialect, null);
        writer.pushQuery(query.sql);
        SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < project.getChildExps().length; i++) {
            RexNode exp = project.getChildExps()[i];
            list.add(
                project.getCluster().rexToSqlTranslator.translate(writer, exp));
        }
        query.sql.getOperands()[SqlSelect.SELECT_OPERAND] = list;
        writer.popQuery(query.sql);
        call.transformTo(query);
    }
}


// End AddProjectToQueryRule.java
