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

import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.core.SaffronSchema;
import net.sf.saffron.ext.ClassSchema;
import openjava.ptree.Expression;
import openjava.ptree.FieldAccess;


/**
 * <code>SalesInMemoryConnection</code> is a Saffron connection to a Java
 * {@link Sales} object. The effect is the same as using a {@link Sales}
 * object directly, but the optimizer uses Saffron's {@link SaffronSchema}
 * mechanism to declare tables.
 */
public class SalesInMemoryConnection implements SaffronConnection
{
    //~ Static fields/initializers --------------------------------------------

    private static final SalesSchema schema = createSchema();

    //~ Instance fields -------------------------------------------------------

    public final SalesInMemory sales = new SalesInMemory();

    //~ Constructors ----------------------------------------------------------

    public SalesInMemoryConnection()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * As required by the {@link SaffronConnection} contract.
     */
    public static SaffronSchema getSaffronSchemaStatic()
    {
        return schema;
    }

    public SaffronSchema getSaffronSchema()
    {
        return schema;
    }

    public Object contentsAsArray(String qualifier,String tableName)
    {
        throw new UnsupportedOperationException(
            "contentsAsArray() should have been replaced");
    }

    private static SalesSchema createSchema()
    {
        return new SalesSchema();
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class SalesSchema extends ClassSchema
    {
        public SalesSchema()
        {
            super(SalesInMemory.class,false);
        }
        protected Expression getTarget(Expression connectionExp)
        {
            return new FieldAccess(connectionExp,"sales");
        }
    }
}


// End Sales.java
