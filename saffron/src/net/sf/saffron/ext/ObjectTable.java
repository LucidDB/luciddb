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

import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.core.SaffronSchema;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;


/**
 * <code>ObjectTable</code> is a component of an {@link ObjectSchema}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 8 February, 2002
 */
public class ObjectTable extends AbstractTable
{
    //~ Constructors ----------------------------------------------------------

    public ObjectTable(SaffronSchema schema,SaffronType rowType)
    {
        super(schema,null,rowType);
    }

    //~ Methods ---------------------------------------------------------------

    public String [] getQualifiedName()
    {
        return new String [] { rowType.toString() };
    }

    /**
     * Records the fact that there is another way to access that a field
     * access or method call. For example, "select from classes as c where
     * c.getName().equals(x)" is the same as "schema.classForName(x)".
     */
    public void addAccessor(String fieldName,String schemaMethodName)
    {
    }

    public SaffronRel toRel(VolcanoCluster cluster,SaffronConnection connection)
    {
        throw new UnsupportedOperationException();
    }
}


// End ObjectTable.java
