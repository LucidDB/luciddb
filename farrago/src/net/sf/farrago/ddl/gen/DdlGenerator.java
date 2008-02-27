/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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
package net.sf.farrago.ddl.gen;

import java.lang.reflect.*;

import java.util.*;
import java.io.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.jmi.*;


/**
 * Base class for DDL generators which use the visitor pattern to generate DDL
 * given a catalog object.
 *
 * <p>Escape rules:<ol>
 *
 * <li>In a SET SCHEMA command, apostrophes
 *     (') and quotes (") enclose the schema name, like this: '"Foo"'. In this
 *     context, apostrophes and quotes must be escaped.</li>
 *
 * <li>CREATE and DROP commands
 *     use quotes (") to enclose the object name. Only quotes are escaped.</li>
 * </ol>
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public abstract class DdlGenerator
    implements ReflectiveVisitor
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final SqlDialect sqlDialect = SqlUtil.eigenbaseDialect;
    protected static final String VALUE_NULL = "NULL";
    protected static final String NL = System.getProperty("line.separator");
    protected static final String SEP = ";" + NL + NL;

    private boolean schemaQualified;
    protected String previousSetSchema;

    private final ReflectiveVisitDispatcher<DdlGenerator, CwmModelElement>
        visitDispatcher =
        ReflectUtil.createDispatcher(
            DdlGenerator.class, CwmModelElement.class);

    private static final List<Class> ADDITIONAL_PARAMETER_TYPES =
        Collections.singletonList((Class) GeneratedDdlStmt.class);

    //~ Methods ----------------------------------------------------------------

    protected abstract JmiModelView getModelView();

    /**
     * Sets whether object names should be qualified with a schema name, if
     * they have one. Default is false.
     *
     * @param schemaQualified whether to qualify object names with schema name
     */
    public void setSchemaQualified(boolean schemaQualified)
    {
        this.schemaQualified = schemaQualified;
    }

    /**
     * Appends a 'SET SCHEMA' command to <code>stmt</code> if
     * <code>schemaName</code> is not null. If <code>evenIfUnchanged</code>
     * is true, does so even if the schema is the same as the previous
     * call to this method.
     *
     * @param stmt Statement to append to
     * @param schemaName Name of schema
     * @param evenIfUnchanged Whether to generate again for same schema name
     *
     * @return whether SET SCHEMA command was generated
     */
    public boolean generateSetSchema(
        GeneratedDdlStmt stmt,
        String schemaName,
        boolean evenIfUnchanged)
    {
        if (schemaName != null
            && (evenIfUnchanged
            || previousSetSchema == null
            || !previousSetSchema.equals(schemaName)))
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SET SCHEMA ");
            sb.append(literal(quote(schemaName)));
            stmt.addStmt(sb.toString());
            previousSetSchema = schemaName;
            return true;
        } else {
            return false;
        }
    }
    
    public void generateCreate(CwmModelElement e, GeneratedDdlStmt stmt)
    {
        generate("create", e, stmt);
    }

    public void generateDrop(CwmModelElement e, GeneratedDdlStmt stmt)
    {
        generate("drop", e, stmt);
    }

    private void generate(
        String method,
        CwmModelElement e,
        GeneratedDdlStmt stmt)
    {
        Method m =
            visitDispatcher.lookupVisitMethod(
                this.getClass(),
                e.getClass(),
                method,
                ADDITIONAL_PARAMETER_TYPES);
        if (m != null) {
            try {
                m.invoke(
                    this,
                    e,
                    stmt);
            } catch (InvocationTargetException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            } catch (IllegalAccessException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            } catch (RuntimeException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            }
        }
    }

    public static String quote(String str)
    {
        return sqlDialect.quoteIdentifier(str);
    }

    public static String literal(String str)
    {
        return sqlDialect.quoteStringLiteral(str);
    }

    public static String unquoteLiteral(String str)
    {
        return sqlDialect.unquoteStringLiteral(str);
    }

    protected static SqlTypeName getSqlTypeName(CwmClassifier classifier)
    {
        //REVIEW: make this work for UDTs
        if (classifier == null) {
            return SqlTypeName.ANY;
        } else {
            String typeName = classifier.getName();
            SqlTypeName stn = SqlTypeName.get(typeName);
            if (stn == null) {
                return SqlTypeName.ANY;
            }
            return stn;
        }
    }

    protected static boolean hasPrimaryKeyConstraint(FemStoredColumn col)
    {
        boolean result = false;

        if (col != null) {
            Collection<FemKeyComponent> keyComponent = col.getKeyComponent();
            if (keyComponent != null) {
                for (FemKeyComponent kc : keyComponent) {
                    if (kc.getKeyConstraint()
                        instanceof FemPrimaryKeyConstraint)
                    {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Converts a set of elements to a string using this generator.
     *
     * <p>If <code>sort</code> is specified, sorts list first so that dependent
     * elements are created after their dependencies.
     *
     * @param exportList List of elements to export
     * @param sort Whether to sort list in dependency order
     * @return DDL script
     */
    public String getExportText(
        List<CwmModelElement> exportList, boolean sort)
    {
        StringBuilder outBuf = new StringBuilder();
        GeneratedDdlStmt stmt = new GeneratedDdlStmt();

        if (sort) {
            final JmiModelView modelView = getModelView();
            final JmiModelGraph modelGraph = modelView.getModelGraph();
            boolean debug = false;
            if (debug) {
                PrintWriter pw = new PrintWriter(System.out);
                JmiObjUtil.dumpGraph(modelView, pw);
                pw.flush();
            }

            // Mapping rules as per farrago/examples/dmv/schemaDependencies.xml
            JmiDependencyMappedTransform transform =
                new JmiDependencyMappedTransform(
                    modelView, false);
            transform.setTieBreaker(new MyComparator());

            transform.setAllByAggregation(
                AggregationKindEnum.COMPOSITE,
                JmiAssocMapping.HIERARCHY);

            transform.setAllByAggregation(
                AggregationKindEnum.NONE,
                JmiAssocMapping.COPY);

            transform.setByRefAssoc(
                lookupAssoc("DependencyClient", modelGraph),
                JmiAssocMapping.CONTRACTION);

            transform.setByRefAssoc(
                lookupAssoc("DependencySupplier", modelGraph),
                JmiAssocMapping.COPY);

            // ignore ownership of views: their dependencies are more important
            transform.setByRefAssocRefined(
                lookupAssoc("ElementOwnership", modelGraph),
                JmiAssocMapping.REMOVAL,
                null,
                lookupClass("LocalView", modelGraph));

            // create schemas before the objects contained in them
            transform.setByRefAssocRefined(
                lookupAssoc("ElementOwnership", modelGraph),
                JmiAssocMapping.COPY,
                lookupClass("LocalSchema", modelGraph),
                null);

            // create method implementations after the operations which specify
            // their interface
            transform.setByRefAssoc(
                lookupAssoc("OperationMethod", modelGraph),
                JmiAssocMapping.COPY);

            JmiDependencyGraph dependencyGraph =
                new JmiDependencyGraph(
                    (Collection) exportList,
                    transform);

            if (debug) {
                PrintWriter pw = new PrintWriter(System.out);
                JmiObjUtil.dumpGraph(dependencyGraph, pw, new NamerImpl());
                pw.flush();
            }

            exportList = new ArrayList<CwmModelElement>();
            JmiDependencyIterator vertexIter =
                new JmiDependencyIterator(dependencyGraph);
            while (vertexIter.hasNext()) {
                JmiDependencyVertex vertex = vertexIter.next();
                exportList.addAll(
                    (Collection) vertex.getElementSet());
            }
        }
        for (CwmModelElement elem : exportList) {
            // proceed if a catalog object has an ddlgen error
            try {
                stmt.clear();
                generateCreate(elem, stmt);
                if (!stmt.isTopLevel()) {
                    continue;
                }
                final String ddl = stmt.toString();
                assert (ddl != null) && !ddl.equals("") : "Do not know how to generate DDL for "
                    + elem.getClass();
                outBuf.append(ddl);
                outBuf.append(SEP);
            } catch (RuntimeException e) {
                throw Util.newInternal(
                    e,
                    "Error while exporting '" + elem + "'");
            }
        }
        return outBuf.toString();
    }

    /**
     * Looks up a named class in the model, fails if not found.
     *
     * @param className Association name
     * @param modelGraph Model graph
     * @return Class, never null
     */
    private RefClass lookupClass(
        String className,
        JmiModelGraph modelGraph)
    {
        JmiClassVertex classVertex =
            modelGraph.getVertexForClassName(className);
        if (classVertex == null) {
            throw new IllegalArgumentException("unknown class " + className);
        }
        return classVertex.getRefClass();
    }

    /**
     * Looks up a named association in the model, fails if not found.
     *
     * @param assocName Association name
     * @param modelGraph Model graph
     * @return Association, never null
     */
    private RefAssociation lookupAssoc(
        String assocName,
        JmiModelGraph modelGraph)
    {
        JmiAssocEdge edge =
            modelGraph.getEdgeForAssocName(assocName);
        if (edge == null) {
            throw new IllegalArgumentException(
                "unknown association " + assocName);
        }
        return edge.getRefAssoc();
    }

    /**
     * Returns whether an object type supports <code>CREATE OR REPLACE</code>
     * operation.
     *
     * @param typeName Name of object type, e.g. "CLUSTERED INDEX"
     * @return whether type supports REPLACE
     */
    protected abstract boolean typeSupportsReplace(String typeName);

    /**
     * Gathers a list of elements in a schema, optionally including elements
     * which don't belong to any schema.
     *
     * @param list                     List to populate
     * @param schemaName               Name of schema
     * @param includeNonSchemaElements Whether to include elements which do not
     *                                 belong to a schema
     * @param catalog                  Catalog
     */
    public abstract void gatherElements(
        List<CwmModelElement> list,
        String schemaName,
        boolean includeNonSchemaElements,
        CwmCatalog catalog);

    /**
     * Outputs the name of an object, optionally qualified by a schema name.
     *
     * @param sb StringBuilder to write to
     *
     * @param schema Schema object belongs to, or null if object does not
     * belong to a schema
     *
     * @param objectName Name of object
     */
    protected void name(
        StringBuilder sb,
        CwmNamespace schema,
        String objectName)
    {
        if (schemaQualified && schema != null) {
            sb.append(quote(schema.getName()));
            sb.append('.');
        }
        sb.append(quote(objectName));
    }

    /**
     * Implementation of {@link org.eigenbase.jmi.JmiObjUtil.Namer} which
     * generates names for objects based on their position in the CWM
     * catalog-schema-object hierarchy.
     *
     * <p>For example, a table's name might be
     * "CATALOG.SALES.EMP (LocalTable)".
     */
    private static class NamerImpl implements JmiObjUtil.Namer
    {
        public String getName(RefObject o)
        {
            StringBuilder buf = new StringBuilder();
            if (o instanceof CwmModelElement) {
                CwmModelElement modelElement = (CwmModelElement) o;
                yy(modelElement, buf);
            } else {
                buf.append(o.toString());
            }
            buf.append('(');
            buf.append(JmiObjUtil.getTypeName(o));
            buf.append(')');
            return buf.toString();
        }

        private void yy(CwmModelElement modelElement, StringBuilder buf)
        {
            CwmNamespace namespace = modelElement.getNamespace();
            if (namespace != null) {
                yy(namespace, buf);
                buf.append('.');
            }
            buf.append(modelElement.getName());
        }
    }

    /**
     * Comparator for schema elements to ensure that export file occurs in
     * an intuitive order.
     */
    private static class MyComparator implements Comparator<RefBaseObject>
    {
        // Priority order of classes.
        private final Class[] classes = {
            // data wrappers first
            FemDataWrapper.class,
            // data server before schema
            FemDataServer.class,
            // next functions and procedures
            CwmProcedure.class,
            // schema after non-schema objects
            CwmSchema.class,
            // index before a view on the same table
            FemLocalIndex.class,
            CwmTable.class,
        };

        public int compare(RefBaseObject o1, RefBaseObject o2)
        {
            // First compare classes. An object sorts earlier if
            // its class is higher in the pecking order.
            int c = compareClass(o1, o2);
            if (c != 0) {
                return c;
            }
            // Next, for objects of the same type, sort by package
            // and name. B.C sorts before B.D but after A.D.
            if (o1 instanceof CwmModelElement
                && o2 instanceof CwmModelElement)
            {
                return compareModelElements(
                    (CwmModelElement) o1,
                    (CwmModelElement) o2);
            }
            // Lastly compare by MofId.
            return o1.refMofId().compareTo(o2.refMofId());
        }

        /**
         * Compares objects by their class. An object sorts earlier
         * if its class is higher in the pecking order.
         */
        private int compareClass(Object o1, Object o2)
        {
            if (o1.getClass() != o2.getClass()) {
                int i1 = findClass(o1);
                int i2 = findClass(o2);
                if (i1 != i2) {
                    return i1 - i2;
                }
            }
            return o1.getClass().getName().compareTo(
                o2.getClass().getName());
        }

        /**
         * Returns the ordinal of an object's class in the pecking
         * order, or {@link Integer#MAX_VALUE} if not found.
         */
        private int findClass(Object o)
        {
            for (int i = 0; i < classes.length; i++) {
                if (classes[i].isInstance(o)) {
                    return i;
                }
            }
            return Integer.MAX_VALUE;
        }

        /**
         * Compares two model elements of the same type by their
         * position in the hierarchy.
         */
        private int compareModelElements(
            CwmModelElement me1,
            CwmModelElement me2)
        {
            CwmNamespace ns1 = me1.getNamespace();
            CwmNamespace ns2 = me2.getNamespace();
            if (ns1 == ns2) {
                return me1.getName().compareTo(me2.getName());
            } else if (ns1 == null) {
                return -1;
            } else if (ns2 == null) {
                return 1;
            } else {
                return compareModelElements(ns1, ns2);
            }
        }
    }
}

// End DdlGenerator.java
