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

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.ImplementableTable;

import openjava.ptree.ParseTree;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.util.Util;


/**
 * Implements the {@link TableAccessRel} relational expression in Java code.
 */
public class JavaTableAccessRel extends TableAccessRel implements JavaLoopRel
{
    public JavaTableAccessRel(
        RelOptCluster cluster,
        ImplementableTable table,
        RelOptConnection connection)
    {
        super(cluster, new RelTraitSet(CallingConvention.JAVA), table, connection);
    }

    public Object clone()
    {
        return this;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        ImplementableTable implementableTable = (ImplementableTable) table;
        implementableTable.implement(this, implementor);
        return null;
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        throw Util.newInternal("should never be called");
    }
}


// End JavaTableAccessRel.java
