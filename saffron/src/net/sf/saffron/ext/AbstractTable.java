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

import net.sf.saffron.core.*;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;


/**
 * A <code>AbstractTable</code> is a partial implementation of {@link
 * SaffronTable}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 3, 2002
 */
public abstract class AbstractTable implements SaffronTable
{
    //~ Instance fields -------------------------------------------------------

    SaffronSchema schema;
    final SaffronType rowType;
    String name;

    //~ Constructors ----------------------------------------------------------

    protected AbstractTable(
        SaffronSchema schema,
        String name,
        SaffronType rowType)
    {
        this.schema = schema;
        this.name = name;
        this.rowType = rowType;
    }

    //~ Methods ---------------------------------------------------------------

    public String getName()
    {
        return name;
    }

    public String [] getQualifiedName()
    {
        return new String [] { name };
    }

    public double getRowCount()
    {
        return 100;
    }

    public SaffronType getRowType()
    {
        return rowType;
    }

    public SaffronSchema getSaffronSchema()
    {
        return schema;
    }
}


// End AbstractTable.java
