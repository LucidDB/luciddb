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

package sales;

import net.sf.saffron.oj.stmt.OJStatement;
import net.sf.saffron.core.SaffronConnection;


/**
 * <code>SalesInMemorySchemaTestCase</code> runs queries which refer to the
 * 'sales' schema using a {@link SalesInMemoryConnection} object.
 * 
 * <p>
 * The plans ought to be the same as those generated for {@link
 * InMemorySalesTestCase}, even the optimizer finds out about the fields in a
 * different way.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 24 March, 2003
 */
public class SalesInMemorySchemaTestCase extends SalesTestCase
{
    static final SalesInMemoryConnection connection = new SalesInMemoryConnection();

    //~ Constructors ----------------------------------------------------------

    public SalesInMemorySchemaTestCase(String s) throws Exception
    {
        super(s);
        arguments = new OJStatement.Argument [] {
            new OJStatement.Argument(
                    "salesDb",
                    connection)
        };
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronConnection getConnection() {
        return connection;
    }
}


// End InMemorySalesTestCase.java
