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

package net.sf.saffron.ext;

import net.sf.saffron.core.SaffronSchema;
import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.sql.SqlDialect;

import javax.sql.DataSource;


/**
 * A <code>JdbcSchema</code> is a schema against a JDBC database and for
 * which, therefore, we will need to generate SQL.
 */
public interface JdbcSchema extends SaffronSchema
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the SQL dialect spoken by this database.
     */
    SqlDialect getSqlDialect();

    /**
     * Returns the JDBC data source contained within a Saffron connection.
     */
    DataSource getDataSource(SaffronConnection connection);
}
