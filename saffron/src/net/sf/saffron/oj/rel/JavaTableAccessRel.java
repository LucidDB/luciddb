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

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.ImplementableTable;
import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.TableAccessRel;
import net.sf.saffron.util.Util;
import openjava.ptree.ParseTree;

/**
 * Implements the {@link TableAccessRel} relational expression in Java code.
 */
public class JavaTableAccessRel extends TableAccessRel implements JavaLoopRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaTableAccessRel(
        VolcanoCluster cluster,
        ImplementableTable table,
        SaffronConnection connection)
    {
        super(cluster,table,connection);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return this;
    }

    // implement SaffronRel
    public ParseTree implement(JavaRelImplementor implementor)
    {
        ImplementableTable implementableTable = (ImplementableTable) table;
        implementableTable.implement(this,implementor);
        return null;
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        throw Util.newInternal("should never be called");
    }
}


// End JavaTableAccessRel.java
