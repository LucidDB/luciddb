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

package net.sf.saffron.ext;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;


/**
 * <code>ObjectTable</code> is a component of an {@link ObjectSchema}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 8 February, 2002
 */
public class ObjectTable extends RelOptAbstractTable
{
    public ObjectTable(
        RelOptSchema schema,
        RelDataType rowType)
    {
        super(schema, null, rowType);
    }

    public String [] getQualifiedName()
    {
        return new String [] { rowType.toString() };
    }

    /**
     * Records the fact that there is another way to access that a field
     * access or method call. For example, "select from classes as c where
     * c.getName().equals(x)" is the same as "schema.classForName(x)".
     */
    public void addAccessor(
        String fieldName,
        String schemaMethodName)
    {
    }

    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        throw new UnsupportedOperationException();
    }
}


// End ObjectTable.java
