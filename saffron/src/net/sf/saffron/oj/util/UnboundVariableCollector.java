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

import java.util.Enumeration;
import java.util.Hashtable;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.VisitorRelVisitor;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;


/**
 * Finds all unbound varaibles in a relational expression
 *
 * @see #collectFromRel
 */
public class UnboundVariableCollector extends RexShuttle
{
    //~ Instance fields -------------------------------------------------------

    Environment env;
    Hashtable mapNameToClass;

    //~ Constructors ----------------------------------------------------------

    UnboundVariableCollector(Environment env)
    {
        this.env = env;
        this.mapNameToClass = new Hashtable();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a description of what variables are unbound inside
     * <code>rel</code>.
     */
    public static UnboundVariableCollector collectFromRel(RelNode rel)
    {
        UnboundVariableCollector unboundVars =
            new UnboundVariableCollector(rel.getCluster().env);
        RelOptUtil.go(
            new VisitorRelVisitor(unboundVars),
            rel);
        return unboundVars;
    }

    /**
     * Returns an argument list <code>value0, ..., valueN</code> containing
     * all variables <code>value<i>i</i>value</code> in this cluster's
     * expression.
     */
    public ExpressionList getArgumentList()
    {
        ExpressionList argumentList = new ExpressionList();
        Enumeration names = mapNameToClass.keys();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            argumentList.add(new Variable(name));
        }
        return argumentList;
    }

    /**
     * Returns a set of assignments to member variables: <code>this.value0 =
     * value0; ...; this.valueN = valueN; return this;</code>.
     */
    public StatementList getAssignmentList()
    {
        StatementList statementList = new StatementList();
        Enumeration names = mapNameToClass.keys();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            statementList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(
                            SelfAccess.makeThis(),
                            name),
                        AssignmentExpression.EQUALS,
                        new Variable(name))));
        }
        return statementList;
    }

    /**
     * Returns a set of declarations of members to hold variables:
     * <code>private Class0 value0; ...; private ClassN valueN;</code>.
     */
    public MemberDeclarationList getMemberDeclarationList()
    {
        MemberDeclarationList memberDeclarationList =
            new MemberDeclarationList();
        Enumeration names = mapNameToClass.keys();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            OJClass clazz = (OJClass) mapNameToClass.get(name);
            memberDeclarationList.add(
                new FieldDeclaration(
                    new ModifierList(ModifierList.PRIVATE),
                    TypeName.forOJClass(clazz),
                    name,
                    null));
        }
        return memberDeclarationList;
    }

    /**
     * Returns a parameter list <code>Class0 value0, ..., ClassN valueN</code>
     * containing all of the unbound variables
     * <code>value<i>i</i>value</code> in this cluster's expression.
     */
    public ParameterList getParameterList()
    {
        ParameterList parameterList = new ParameterList();
        Enumeration names = mapNameToClass.keys();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            OJClass clazz = (OJClass) mapNameToClass.get(name);
            parameterList.add(new Parameter(
                    TypeName.forOJClass(clazz),
                    name));
        }
        return parameterList;
    }

    // todo: jhyde 2003/12/10 This doesn't work, and won't until we introduce
    //   RexParameter
    public RexNode visit(RexCorrelVariable v)
    {
        final Environment.VariableInfo info = env.lookupBind(v.toString());
        if (info != null) {
            // variable's type is known in the enclosing environment,
            // therefore it must be inherited from that environment
            mapNameToClass.put(
                v.toString(),
                info.getType());
        }
        return v;
    }
}
