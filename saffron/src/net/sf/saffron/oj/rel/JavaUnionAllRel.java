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

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.UnionRel;
import net.sf.saffron.util.Util;

import openjava.ptree.StatementList;


/**
 * <code>JavaUnionAllRel</code> implements a {@link UnionRel} inline, without
 * eliminating duplicates.
 */
public class JavaUnionAllRel extends UnionRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaUnionAllRel(VolcanoCluster cluster,SaffronRel [] inputs)
    {
        super(cluster,inputs,true);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new JavaUnionAllRel(cluster,inputs);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = getRows();
        double dCpu = 0;
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    // Generate
    //   for (int i = 0; i < a.length; i++) {
    //      T row = a[i];
    //      stuff
    //   }
    //   for (int j = 0; j < b.length; j++) {
    //      T row = b[j];
    //      stuff
    //   }
    //
    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:
            for (int i = 0; i < inputs.length; i++) {
                Util.discard(implementor.implementChild(this,i,inputs[i]));
            }
            return null;
        default:
            if (ordinal >= inputs.length) {
                throw Util.newInternal("implement: ordinal=" + ordinal);
            }

            // Generate
            //   <<child loop>> {
            //     Type var = <<child row>>
            //     <<parent-handler>>
            //   }
            //   <<next child>>
            StatementList stmtList = implementor.getStatementList();
            implementor.bind(this,inputs[ordinal]);
            implementor.generateParentBody(this,stmtList);
            return null;
        }
    }
}


// End JavaUnionAllRel.java
