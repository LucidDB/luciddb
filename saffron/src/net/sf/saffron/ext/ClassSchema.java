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
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.util.Util;
import openjava.ptree.Expression;
import openjava.ptree.FieldAccess;

import java.lang.reflect.Field;


/**
 * A <code>ClassSchema</code> is a schema whose tables are reflections of the
 * the public fields of a given class.
 */
public class ClassSchema implements SaffronSchema
{
    //~ Instance fields -------------------------------------------------------

    private final Class clazz;
    private final boolean ignoreCase;

    //~ Constructors ----------------------------------------------------------

    public ClassSchema(Class clazz,boolean ignoreCase)
    {
        this.clazz = clazz;
        this.ignoreCase = ignoreCase;
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronTable getTableForMember(String [] names)
    {
        assert(names.length == 1);
        String name = names[0];
        final Field field = findField(name);
        if (field == null) {
            return null;
        }
        final Class rowType = Util.guessRowType(field.getType());
        SaffronType type = getTypeFactory().createJavaType(rowType);
        return new AbstractTable(this,name,type) {
                public SaffronRel toRel(
                    VolcanoCluster cluster,
                    SaffronConnection connection)
                {
                    Util.pre(cluster != null, "cluster != null");
                    Util.pre(connection != null, "connection != null");

                    final OJConnectionRegistry.ConnectionInfo info =
                            OJConnectionRegistry.instance.get(connection,true);
                    final Expression connectionExpr = info.expr;
                    final FieldAccess expr = new FieldAccess(
                            getTarget(connectionExpr),
                            field.getName());
                    final JavaRexBuilder javaRexBuilder = (JavaRexBuilder)
                            cluster.rexBuilder;
                    final RexNode rex = javaRexBuilder.makeJava(info.env, expr);
                    final ExpressionReaderRel exprReader = new ExpressionReaderRel(cluster, rex, getRowType());
                    if (true) {
                        return exprReader; // todo: cleanup
                    }
                    final SaffronField [] exprReaderFields = exprReader.getRowType().getFields();
                    assert exprReaderFields.length == 1;
                    // Create a project "$f0.name, $f0.empno, $f0.gender".
                    RexNode fieldAccess = cluster.rexBuilder.makeInputRef(exprReaderFields[0].getType(), 0);
                    final SaffronField [] fields = fieldAccess.getType().getFields();
                    final String [] fieldNames = new String[fields.length];
                    final RexNode [] exps = new RexNode[fields.length];
                    for (int i = 0; i < exps.length; i++) {
                        exps[i] = cluster.rexBuilder.makeFieldAccess(fieldAccess, i);
                        fieldNames[i] = fields[i].getName();
                    }
                    final ProjectRel project = new ProjectRel(cluster,
                            exprReader, exps, fieldNames,
                            ProjectRel.Flags.Boxed);
                    return project;
                }
            };
    }

    public SaffronTypeFactory getTypeFactory()
    {
        return SaffronTypeFactoryImpl.threadInstance();
    }

    public void registerRules(SaffronPlanner planner) throws Exception
    {
    }

    /**
     * Given the expression which yields the current connection, returns an
     * expression which yields the object which holds the schema data.
     *
     * <p>By default, returns the connection expression. So if the connection
     * expression is <code>Variable("connection")</code>, it will return the
     * same variable, and the planner will expect to be able to cast this value
     * to the required class and find a field for each 'table' in the
     * schema.</p>
     */
    protected Expression getTarget(Expression connectionExp)
    {
        return connectionExp;
    }

    private Field findField(String name)
    {
        Field field;
        try {
            field = clazz.getField(name);
        } catch (NoSuchFieldException e) {
            field = null;
        } catch (SecurityException e) {
            field = null;
        }
        if ((field != null) || !ignoreCase) {
            return field;
        }
        final Field [] fields = clazz.getFields();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getName().equalsIgnoreCase(name)) {
                return fields[i];
            }
        }
        return null;
    }
}


// End ClassSchema.java
