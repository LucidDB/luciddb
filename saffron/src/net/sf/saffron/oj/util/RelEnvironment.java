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

package net.sf.saffron.oj.util;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.VolcanoQuery;
import net.sf.saffron.rel.SaffronRel;
import openjava.mop.Environment;
import openjava.mop.OJClass;

/**
 * <code>RelEnvironment</code> defines the set of variables available to a
 * {@link SaffronRel relational expression}.
 */
public class RelEnvironment extends Environment
{
    //~ Instance fields -------------------------------------------------------

    SaffronRel rel;

    //~ Constructors ----------------------------------------------------------

    public RelEnvironment(SaffronRel rel)
    {
        super(rel.getCluster().env);
        this.rel = rel;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Environment
    public String getPackage()
    {
        return parent.getPackage();
    }

    // implement Environment
    public void bindVariable(String name,VariableInfo info)
    {
        throw new UnsupportedOperationException();
    }

    public VariableInfo lookupBind(String name)
    {
        if (name.startsWith(VolcanoQuery.correlPrefix)) {
            SaffronRel correl = rel.getQuery().lookupCorrel(name);
            if (correl != null) {
                final SaffronType rowType = correl.getRowType();
                return new BasicVariableInfo(OJUtil.typeToOJClass(rowType));
            }
        }
        int i = OptUtil.getInputOrdinal(name);
        if (i >= 0) {
            SaffronRel input = rel.getInput(i);
            SaffronType rowType = input.getRowType();
            final OJClass rowClass = OJUtil.typeToOJClass(rowType);
            return new BasicVariableInfo(rowClass);
        } else {
            return parent.lookupBind(name);
        }
    }

    // implement Environment
    public OJClass lookupClass(String name)
    {
        return parent.lookupClass(name);
    }

    // implement Environment
    public void record(String name,OJClass clazz)
    {
        throw new UnsupportedOperationException();
    }

    // implement Environment
    public String toString()
    {
        return "RelEnvironment: rel=" + rel.toString();
    }
}


// End RelEnvironment.java
