/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.rel.jdbc;

import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.sql.SqlNodeList;
import net.sf.saffron.sql.SqlSelect;
import net.sf.saffron.sql.SqlWriter;
import net.sf.saffron.sql.parser.ParserPosition;


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
class AddProjectToQueryRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    AddProjectToQueryRule()
    {
        super(
            new RuleOperand(
                ProjectRel.class,
                new RuleOperand [] { new RuleOperand(JdbcQuery.class,null) }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
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
        SqlWriter writer = new SqlWriter(query.dialect,null);
        writer.pushQuery(query.sql);
        SqlNodeList list = new SqlNodeList(ParserPosition.ZERO);
        for (int i = 0; i < project.getChildExps().length; i++) {
            RexNode exp = project.getChildExps()[i];
            list.add(project.getCluster().rexToSqlTranslator.translate(writer,exp));
        }
        query.sql.getOperands()[SqlSelect.SELECT_OPERAND] = list;
        writer.popQuery(query.sql);
        call.transformTo(query);
    }
}


// End AddProjectToQueryRule.java
