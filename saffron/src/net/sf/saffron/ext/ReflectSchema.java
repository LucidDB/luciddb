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

package net.sf.saffron.ext;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.OJConnectionRegistry;
import net.sf.saffron.oj.rel.ExpressionReaderRel;
import net.sf.saffron.oj.util.JavaRexBuilder;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.util.Util;
import openjava.ptree.FieldAccess;

import java.lang.reflect.Field;


/**
 * Schema which uses reflection to find definitions of tables.
 *
 * <p>Every public member (field or method) which implements
 * {@link SaffronTable} can be used as a relation. Consider the following:
 * <blockquote>
 *
 * <pre>class SalesSchema extends {@link ReflectSchema} {
 *     public {@link SaffronTable} emps = {@link JdbcTable}(this, "EMP");
 *     public {@link SaffronTable} depts = {@link JdbcTable}(this, "DEPT");
 * }
 * class SalesConnection extends {@link SaffronConnection} {
 *     public static {@link SaffronSchema} getSaffronSchema() {
 *         return new SalesSchema();
 *     }
 * }
 * SalesConnection sales;
 * Emp[] femaleEmps = (select from sales.emps as emp where emp.gender.equals("F"));
 * </pre>
 *
 * </blockquote>The expression <code>sales.emps</code> is a valid table because
 * (a) sales is a {@link SaffronConnection},
 * (b) its static method <code>getSaffronSchema()</code> returns a
 *     <code>ReflectSchema</code>, and
 * (c) <code>ReflectSchema</code> has a public field <code>emps</code> of type
 *     {@link SaffronTable}.</p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public class ReflectSchema implements SaffronSchema
{
    //~ Instance fields -------------------------------------------------------

    public final SaffronTypeFactoryImpl typeFactory =
        new SaffronTypeFactoryImpl();
    private final Object target;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a schema whose 'tables' are the public fields in a given
     * object.
     */
    public ReflectSchema(Object target)
    {
        this.target = target;
    }

    /**
     * Creates a schema whose 'tables' are its own public fields.
     */
    public ReflectSchema()
    {
        this.target = this;
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronTable getTableForMember(String [] names)
    {
        assert(names.length == 1);
        final String name = names[0];
        try {
            Class clazz = target.getClass();
            Field field = clazz.getField(name);
            Object o = field.get(target);
            if (o instanceof SaffronTable) {
                return (SaffronTable) o;
            }
            final Class fieldClazz = Util.guessRowType(o.getClass());
            if (fieldClazz != null) {
                final SaffronType fieldType =
                    typeFactory.createJavaType(fieldClazz);

                // todo: make this a real class; ObjectTable looks similar
                return new SaffronTable() {
                        public SaffronSchema getSaffronSchema()
                        {
                            return ReflectSchema.this;
                        }

                        public SaffronType getRowType()
                        {
                            return fieldType;
                        }

                        public double getRowCount()
                        {
                            return 10;
                        }

                        public String [] getQualifiedName()
                        {
                            return new String [] { name };
                        }

                        public SaffronRel toRel(
                            VolcanoCluster cluster,
                            SaffronConnection connection)
                        {
                            OJConnectionRegistry.ConnectionInfo connectionInfo =
                                    OJConnectionRegistry.instance.get(connection);
                            final FieldAccess expr = new FieldAccess(
                                    connectionInfo.expr,
                                    name);
                            final JavaRexBuilder javaRexBuilder = (JavaRexBuilder)
                                    cluster.rexBuilder;
                            final RexNode rex = javaRexBuilder.makeJava(
                                    connectionInfo.env, expr);
                            return new ExpressionReaderRel(cluster,
                                cluster.rexBuilder.makeFieldAccess(rex, name),
                                getRowType());
                        }
                    };
            }
            throw new Error(name + " is not a Table");
        } catch (NoSuchFieldException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    public SaffronTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    public void registerRules(SaffronPlanner planner) throws Exception
    {
        // this implementation of Schema doesn't have any rules
    }
}


// End ReflectSchema.java
