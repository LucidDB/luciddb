/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package sales;

import net.sf.saffron.ext.ClassSchema;
import net.sf.saffron.oj.stmt.OJStatement;

import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptSchema;


/**
 * <code>InMemorySalesTestCase</code> runs queries which refer to the 'sales'
 * schema using a {@link SalesInMemory} object.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 April, 2002
 */
public class InMemorySalesTestCase extends SalesTestCase
{
    private final SalesInMemory salesInMemory = new SalesInMemory();
    private final ClassSchema classSchema =
        new ClassSchema(
            salesInMemory.getClass(),
            false);
    private final RelOptConnection saffronConnection =
        new RelOptConnection() {
            public RelOptSchema getRelOptSchema()
            {
                return classSchema;
            }

            public Object contentsAsArray(
                String qualifier,
                String tableName)
            {
                return null;
            }
        };

    public InMemorySalesTestCase(String s)
        throws Exception
    {
        super(s);
        arguments =
            new OJStatement.Argument [] {
                new OJStatement.Argument("salesDb", salesInMemory)
            };
    }

    public RelOptConnection getConnection()
    {
        return saffronConnection;
    }
}


// End InMemorySalesTestCase.java
