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

import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.ext.JdbcSchema;
import net.sf.saffron.ext.JdbcTable;
import net.sf.saffron.oj.rel.JavaTableAccessRel;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.sql.SqlIdentifier;


/**
 * A <code>CreateSqlQueryRule</code> converts a branch of the tree of
 * relational expressions into a {@link JdbcQuery} (a SQL statement to be
 * executed against a JDBC data source). A parse tree cannot be converted if
 * it contains references to tables in different databases, or non-JDBC
 * relations (for example, Java arrays in a from clause), or Java-specific
 * constructs (for example, method-calls). For now, it works bottom up. We
 * convert a {@link JavaTableAccessRel} to a {@link JdbcQuery}. Then we grow
 * it by grafting on filters to the WHERE clause, projections in the SELECT
 * list, and so forth.
 */
class TableAccessToQueryRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    TableAccessToQueryRule()
    {
        super(new RuleOperand(JavaTableAccessRel.class,null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
    {
        JavaTableAccessRel javaTableAccess = (JavaTableAccessRel) call.rels[0];
        if (!(javaTableAccess.getTable() instanceof JdbcTable)) {
            return;
        }
        JdbcTable table = (JdbcTable) javaTableAccess.getTable();
        JdbcSchema schema = (JdbcSchema) table.getSaffronSchema();
        final SaffronConnection connection = javaTableAccess.getConnection();
        JdbcQuery query = new JdbcQuery(
                javaTableAccess.getCluster(),
                javaTableAccess.getRowType(),
                connection,
                schema.getSqlDialect(),
                null,
                schema.getDataSource(connection));
        query.sql.addFrom(new SqlIdentifier(new String [] { table.getName() }, null));
        call.transformTo(query);
    }
}


// End TableAccessToQueryRule.java
