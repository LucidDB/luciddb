/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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
import net.sf.saffron.rel.FilterRel;
import net.sf.saffron.rex.RexToSqlTranslator;
import net.sf.saffron.sql.SqlNode;
import net.sf.saffron.sql.SqlWriter;


/**
 * <code>AddFilterToQueryRule</code> grafts a {@link FilterRel} onto a {@link
 * JdbcQuery}.
 * 
 * <p> This rule only works if the query's select clause is "&#42;". If you
 * start with a {@link FilterRel} on a {@link net.sf.saffron.rel.ProjectRel} on
 * a {@link net.sf.saffron.oj.rel.JavaTableAccessRel}, this will not be the
 * case. You can fix it by pushing the filter through the project.  (todo:
 * Implement a rule to do this.)  </p>
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 */
class AddFilterToQueryRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    AddFilterToQueryRule()
    {
        super(
            new RuleOperand(
                FilterRel.class,
                new RuleOperand [] { new RuleOperand(JdbcQuery.class,null) }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
    {
        FilterRel filter = (FilterRel) call.rels[0];
        JdbcQuery oldQuery = (JdbcQuery) call.rels[1];
        if (oldQuery.sql.getSelectList() != null) {
            return;
        }
        JdbcQuery query = (JdbcQuery) oldQuery.clone();
        SqlWriter writer = new SqlWriter(oldQuery.dialect,null);
        final RexToSqlTranslator translator = new RexToSqlTranslator();
        writer.pushQuery(query.sql);
        final SqlNode sqlCondition = translator.translate(writer,
                filter.condition);
        query.sql.addWhere(sqlCondition);
        writer.popQuery(query.sql);
        call.transformTo(query);
    }
}


// End AddFilterToQueryRule.java
