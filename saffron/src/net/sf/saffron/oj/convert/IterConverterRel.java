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

package net.sf.saffron.oj.convert;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.util.*;

import openjava.mop.*;

import openjava.ptree.*;


/**
 * <code>IterConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#ITERATOR_ORDINAL}.
 */
public class IterConverterRel extends ConverterRel
{
    //~ Constructors ----------------------------------------------------------

    public IterConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public Object clone()
    {
        return new IterConverterRel(this.cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public CallingConvention getConvention()
                {
                    return CallingConvention.ITERATOR;
                }

                public ConverterRel convert(SaffronRel rel)
                {
                    return new IterConverterRel(rel.getCluster(),rel);
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERABLE));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.COLLECTION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.VECTOR));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ENUMERATION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.RESULT_SET));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.ITERABLE_ORDINAL:
            return implementIterable(implementor,ordinal);
        case CallingConvention.COLLECTION_ORDINAL:
        case CallingConvention.VECTOR_ORDINAL:
            return implementCollection(implementor,ordinal);
        case CallingConvention.ENUMERATION_ORDINAL:
            return implementEnumeration(implementor,ordinal);
        case CallingConvention.RESULT_SET_ORDINAL:
            return implementResultSet(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    /**
     * Returns the name of the method in {@link java.sql.ResultSet} for
     * retrieving a particular type. For example, <code>getInt</code>
     * accesses <code>int</code>, <code>getString</code> accesses
     * <code>String</code>, <code>getObject</code> accesses most other kinds
     * of Object.
     */
    private String getResultSetAccessorMethod(OJClass clazz)
    {
        if (clazz.isPrimitive()) {
            return "get" + toInitcap(clazz.getName());
        } else if (Util.clazzString.isAssignableFrom(clazz)) {
            return "getString";
        } else {
            return "getObject";
        }
    }

    private Object implementCollection(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:

            // Generate
            //   <<exp>>.iterator()
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new MethodCall(exp,"iterator",null);
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementEnumeration(
        RelImplementor implementor,
        int ordinal)
    {
        switch (ordinal) {
        case -1:

            // Generate
            //   new saffron.runtime.EnumerationIterator(<<child>>)
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new AllocationExpression(
                OJClass.forClass(EnumerationIterator.class),
                new ExpressionList(exp));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementIterable(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:

            // Generate
            //   <<exp>>.iterator()
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new MethodCall(exp,"iterator",null);
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementResultSet(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Object o = implementor.implementChild(this,0,child);
            StatementList methodBody = new StatementList();
            Variable varResultSet = new Variable("resultSet");
            OJClass rowClass = OJUtil.typeToOJClass(rowType);
            if (rowType.isProject()) {
                Variable varRow = implementor.newVariable();
                methodBody.add(
                    new VariableDeclaration(
                        TypeName.forOJClass(rowClass),
                        varRow.toString(),
                        new AllocationExpression(
                            rowClass,
                            new ExpressionList())));

                // <<RowType>> row = new <<RowType>>();
                // row.fieldA = resultSet.get<<Type>>(1);
                // ...
                // return row;
                OJField [] fields = rowClass.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    OJField field = fields[i];
                    methodBody.add(
                        new ExpressionStatement(
                            new AssignmentExpression(
                                new FieldAccess(varRow,field.getName()),
                                AssignmentExpression.EQUALS,
                                new MethodCall(
                                    varResultSet,
                                    getResultSetAccessorMethod(
                                        field.getType()),
                                    new ExpressionList(
                                        Literal.makeLiteral(i + 1))))));
                }
                methodBody.add(new ReturnStatement(varRow));
            } else {
                methodBody.add(
                    new ReturnStatement(
                        new AllocationExpression(
                            rowClass,
                            new ExpressionList(varResultSet))));
            }
            return new AllocationExpression(
                TypeName.forOJClass(OJClass.forClass(ResultSetIterator.class)),
                new ExpressionList(
                    new CastExpression(
                        TypeName.forOJClass(Util.clazzResultSet),
                        (Expression) o)),
                new MemberDeclarationList(
                    new MethodDeclaration(
                        new ModifierList(ModifierList.PUBLIC),
                        TypeName.forOJClass(Util.clazzObject),
                        "makeRow",
                        new ParameterList(),
                        new TypeName [] {
                            TypeName.forOJClass(Util.clazzSQLException)
                        },
                        methodBody)));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private String toInitcap(String s)
    {
        return s.substring(0,1).toUpperCase() + s.substring(1);
    }
}


// End IterConverterRel.java
