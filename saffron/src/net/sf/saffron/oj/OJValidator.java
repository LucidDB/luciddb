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

package net.sf.saffron.oj;

import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.VariableBinder;

import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.util.Util;


/**
 * <code>OJValidator</code> makes sure that an expression is valid. It
 * produces more meaningful error messages than code further down the line
 * (for example {@link net.sf.saffron.oj.xlat.OJQueryExpander}) would
 * produce.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 25 April, 2002
 */
public class OJValidator extends VariableBinder
{
    public OJValidator(Environment env)
    {
        super(env);
    }

    public Expression evaluateUp(FieldAccess p)
        throws ParseTreeException
    {
        if (p instanceof TableReference) {
            // TableReference is valid by construction
            return super.evaluateUp(p);
        }
        Environment env = getEnvironment();
        String name = p.getName();
        if (!fieldExists(
                    p.getReference(),
                    name,
                    env)) {
            OJClass reftype = Toolbox.getType(
                    env,
                    p.getReference());
            Signature signature = new Signature(name);
            throw Util.newInternal("no " + signature
                + " is accessible in type '" + reftype + "'");
        }
        return super.evaluateUp(p);
    }

    public Expression evaluateUp(MethodCall p)
        throws ParseTreeException
    {
        Environment env = getEnvironment();
        ParseTree ref = p.getReferenceExpr();
        if (ref == null) {
            ref = p.getReferenceType();
        }
        if (ref == null) {
            ref = SelfAccess.constantThis();
        }
        OJClass refType = Toolbox.getType(env, ref);
        ExpressionList args = p.getArguments();
        OJClass [] argTypes = new OJClass[args.size()];
        for (int i = 0; i < argTypes.length; i++) {
            argTypes[i] = Toolbox.getType(
                    env,
                    args.get(i));
        }
        String name = p.getName();
        OJClass situation = env.lookupClass(env.currentClassName());
        OJMethod method = null;
        try {
            method = refType.getAcceptableMethod(name, argTypes, situation);
        } catch (NoSuchMemberException e) {
            Signature signature = new Signature(name, argTypes);
            throw Util.newInternal(e, "no " + signature + " found");
        }
        if (method == null) {
            Signature signature = new Signature(name, argTypes);
            throw Util.newInternal("no " + signature + " found");
        }
        return super.evaluateUp(p);
    }

    public TypeName evaluateUp(TypeName p)
        throws ParseTreeException
    {
        Environment env = getEnvironment();
        String qname = env.toQualifiedName(p.getName());
        OJClass clazz = env.lookupClass(
                qname,
                p.getDimension());
        if (clazz == null) {
            throw Util.newInternal("unknown type " + p.toString());
        }
        return super.evaluateUp(p);
    }

    private boolean fieldExists(
        ParseTree expr,
        String name,
        Environment env)
    {
        RelOptTable table = Util.getTable(env, expr, null, name);
        if (table != null) {
            return true;
        }
        OJClass situation = env.lookupClass(env.currentClassName());
        OJClass exprType = Toolbox.getType(env, expr);
        try {
            OJField field = exprType.getField(name, situation);
            if (field != null) {
                return true;
            }
        } catch (NoSuchMemberException e) {
            return false;
        }
        return false;
    }
}


// End OJValidator.java
