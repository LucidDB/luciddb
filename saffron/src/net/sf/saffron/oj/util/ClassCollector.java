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

import openjava.mop.Environment;
import openjava.mop.OJClass;

import openjava.ptree.Expression;
import openjava.ptree.ParseTreeException;
import openjava.ptree.TypeName;
import openjava.ptree.Variable;

import openjava.ptree.util.ScopeHandler;

import java.util.HashSet;


/**
 * <code>ClassCollector</code> walks over an expression tree, calling {@link
 * openjava.ptree.Expression#getType} and {@link
 * openjava.ptree.Expression#getRowType} on each node, and forming a set of
 * distinct types.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 2 October, 2001
 */
public class ClassCollector extends ScopeHandler
{
    //~ Instance fields -------------------------------------------------------

    private HashSet classSet = new HashSet();

    //~ Constructors ----------------------------------------------------------

    public ClassCollector(Environment env)
    {
        super(env);
    }

    //~ Methods ---------------------------------------------------------------

    public OJClass [] getClasses()
    {
        return (OJClass []) classSet.toArray(new OJClass[0]);
    }

    public TypeName evaluateDown(TypeName p) throws ParseTreeException
    {
        Environment env = getEnvironment();
        OJClass clazz = env.lookupClass(env.toQualifiedName(p.getName()));
        register(clazz);
        return super.evaluateDown(p);
    }

    private void register(OJClass clazz)
    {
        if (clazz == null) {
            return;
        }
        while (clazz.getComponentType() != null) {
            clazz = clazz.getComponentType();
        }
        classSet.add(clazz);
    }
}


// End ClassCollector.java
