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

package net.sf.saffron.oj.xlat;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.util.Util;

import openjava.mop.Environment;
import openjava.mop.OJClass;

import openjava.ptree.*;

import openjava.ptree.util.VariableBinder;


/**
 * Replaces references to the virtual members of {@link SaffronSchema}
 * expressions.
 * 
 * <p>
 * Suppose that the variable <code>schema</code> implements {@link
 * SaffronSchema}. Then field accesses of the form <code>schema.field</code>,
 * and method calls of the form <code>schema.method(arg0,...)</code>, become
 * Java fragments like <code>(Sales.Emp[])
 * schema.contentsAsArray("emps")</code>.
 * </p>
 * 
 * <p>
 * See {@link SaffronConnection#contentsAsArray} for how these fragments are
 * converted into efficient code.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 11 November, 2001
 */
public class OJSchemaExpander extends VariableBinder
{
    //~ Constructors ----------------------------------------------------------

    public OJSchemaExpander(Environment env)
    {
        super(env);
    }

    //~ Methods ---------------------------------------------------------------

    public Expression evaluateDown(FieldAccess p) throws ParseTreeException
    {
        super.evaluateDown(p);
        Expression refexpr = p.getReferenceExpr(); // e.g. "sales"
        if (refexpr != null) {
            Environment env = getEnvironment();
            String name = p.getName(); // e.g. "emps"
            String qualifier = null;
            if (p instanceof TableReference) {
                qualifier = ((TableReference) p).getQualifier();
            }
            SaffronTable table = Util.getTable(env,refexpr,qualifier,name);
            if (table != null) {
                // (Emp[]) sales.contentsAsArray("emp")
                return new AliasedExpression(
                    new CastExpression(
                        OJClass.arrayOf(
                            OJUtil.typeToOJClass(table.getRowType())),
                        new MethodCall(
                            refexpr,
                            "contentsAsArray",
                            new ExpressionList(
                                (qualifier == null) ? Literal.constantNull()
                                                    : Literal.makeLiteral(
                                    qualifier),
                                Literal.makeLiteral(name)))),
                    name);
            }
        }
        return p;
    }
}


// End OJSchemaExpander.java
