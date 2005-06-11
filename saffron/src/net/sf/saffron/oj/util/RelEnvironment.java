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

package net.sf.saffron.oj.util;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.Expression;

import org.eigenbase.util.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;


/**
 * <code>RelEnvironment</code> defines the set of variables available to a
 * {@link RelNode relational expression}.
 */
public class RelEnvironment extends Environment
{
    //~ Instance fields -------------------------------------------------------

    RelNode rel;

    //~ Constructors ----------------------------------------------------------

    public RelEnvironment(RelNode rel)
    {
        super(rel.getCluster().getEnv());
        this.rel = rel;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Environment
    public String getPackage()
    {
        return parent.getPackage();
    }

    // implement Environment
    public void bindVariable(
        String name,
        VariableInfo info)
    {
        throw new UnsupportedOperationException();
    }

    public VariableInfo lookupBind(String name)
    {
        if (name.startsWith(RelOptQuery.correlPrefix)) {
            RelNode correl = rel.getQuery().lookupCorrel(name);
            if (correl != null) {
                final RelDataType rowType = correl.getRowType();
                return new BasicVariableInfo(
                    OJUtil.typeToOJClass(
                        rowType,
                        rel.getCluster().getTypeFactory()));
            }
        }
        int i = RelOptUtil.getInputOrdinal(name);
        if (i >= 0) {
            RelNode input = rel.getInput(i);
            RelDataType rowType = input.getRowType();
            final OJClass rowClass = OJUtil.typeToOJClass(
                rowType,
                rel.getCluster().getTypeFactory());
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
    public void record(
        String name,
        OJClass clazz)
    {
        throw new UnsupportedOperationException();
    }

    // implement Environment
    public String toString()
    {
        return "RelEnvironment: rel=" + rel.toString();
    }

    public static OJClass ojClassForExpression(
        RelNode rel,
        Expression exp)
    {
        try {
            return exp.getType(new RelEnvironment(rel));
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
    }
}


// End RelEnvironment.java
