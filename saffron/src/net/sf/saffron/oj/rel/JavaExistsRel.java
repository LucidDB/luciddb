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
import net.sf.saffron.rel.DistinctRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;

import openjava.mop.Toolbox;

import openjava.ptree.StatementList;


/**
 * <code>JavaExistsRel</code> implements {@link DistinctRel} inline for the
 * special case that the input relation has zero columns.
 */
public class JavaExistsRel extends DistinctRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaExistsRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
        assert(child.getRowType().getFieldCount() == 0);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new JavaExistsRel(cluster,child);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        // cheaper than JavaDistinct
        double dRows = 1;

        // cheaper than JavaDistinct
        double dCpu = dRows;

        // cheaper than JavaDistinct
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:// Generate
            //     <<parent>>
            return implementor.implementChild(this,0,child);
        case 0: // called from child
         {
            // Generate
            //   <<parent-handler>>
            //   break;
            StatementList stmtList = implementor.getStatementList();
            implementor.generateParentBody(this,stmtList);
            stmtList.add(implementor.getExitStatement());
            return null;
        }
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End JavaExistsRel.java
