/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.util.*;


/**
 * Generates DDL statements from catalog objects.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDdlGenerator
    extends DdlGenerator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final List<String> NON_REPLACEABLE_TYPE_NAMES =
        Arrays.asList(
            "INDEX",
            "CLUSTERED INDEX");

    //~ Instance fields --------------------------------------------------------

    protected final JmiModelView modelView;
    protected boolean uglyViews;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a DDL generator.
     *
     * <p>The <code>modelView</code> parameter can be null if you are generating
     * DDL for a single object. A model view is required if calling {@link
     * DdlGenerator#gatherElements} or {@link #getExportText(java.util.List,
     * boolean)} with <code>sort=true</code>.
     *
     * @param sqlDialect SQL dialect
     * @param modelView Model graph
     */
    public FarragoDdlGenerator(
        SqlDialect sqlDialect,
        JmiModelView modelView)
    {
        super(sqlDialect);
        this.modelView = modelView;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets whether we output the user-defined SQL for VIEWS, which is
     * prettier but may be incorrect, or if we output the correct (but ugly)
     * system-expanded rewrite of the view definition. Default is false.
     *
     * @param uglyViews whether to make VIEW SQL ugly
     */
    public void setUglyViews(boolean uglyViews)
    {
        this.uglyViews = uglyViews;
    }

    private RefClass findRefClass(Class<? extends RefObject> clazz)
    {
        JmiClassVertex vertex =
            modelView.getModelGraph().getVertexForJavaInterface(clazz);
        return vertex.getRefClass();
    }

    public <T extends RefObject> Collection<T> allOfClass(Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        return (Collection<T>) refClass.refAllOfClass();
    }

    public <T extends RefObject> Collection<T> allOfType(Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        return (Collection<T>) refClass.refAllOfType();
    }

    public String getExportText(List<CwmModelElement> exportList, boolean sort)
    {
        if (sort) {
            assert modelView != null;
        }
        return super.getExportText(exportList, sort);
    }

    protected boolean typeSupportsReplace(String typeName)
    {
        return !NON_REPLACEABLE_TYPE_NAMES.contains(typeName);
    }

    protected JmiModelView getModelView()
    {
        return modelView;
    }

    /**
     * Finds a list of elements and their dependencies for a given
     * schema and catalog.
     *
     * @param list - List to which found elements are added.
     * @param schemaName - Schema to limit list, null to add all schemas.
     * @param includeNonSchemaElements - Will include every other type
     * of direct-child object in the catalog.
     * @param catalog - Catalog object to search.
     */
    public void gatherElements(
        List<CwmModelElement> list,
        String schemaName,
        boolean includeNonSchemaElements,
        CwmCatalog catalog)
    {
        assert modelView != null;
        for (CwmModelElement element : catalog.getOwnedElement()) {
            if (element instanceof CwmSchema) {
                CwmSchema schema = (CwmSchema) element;
                // Note: ignore schema name if null.
                if (schemaName == null || schema.getName().equals(schemaName)) {
                    list.add(schema);
                    for (CwmModelElement element2 : schema.getOwnedElement()) {
                        list.add(element2);
                    }
                }
            } else if (includeNonSchemaElements) {
                list.add(element);
            }
        }

        // Include all dependencies. We don't want to generate DDL for them,
        // but they are necessary to ensure that views are created in the
        // right order, among other things.
        // NOTE: We include dependencies of all objects in all schemas. This
        // will not produce incorrect results, but may be a performance issue.
        for (CwmDependency dependency : allOfClass(CwmDependency.class)) {
            list.add(dependency);
        }

        // Likewise operations.
        list.addAll(allOfClass(CwmOperation.class));

        if (includeNonSchemaElements) {
            // Note these are independent of catalog.
            list.addAll(allOfType(FemDataServer.class));
            list.addAll(allOfType(FemDataWrapper.class));
            list.addAll(allOfType(FemUser.class));
            // TODO: ks 25-Nov-2010: extend create for Role to include admin;
            // requires that we become aware of GRANT elements since the
            // admin isn't directly stored with the FemRole.
            list.addAll(allOfType(FemRole.class));
            list.addAll(allOfType(FemLabel.class));
        }
    }

    protected void createHeader(
        SqlBuilder sb,
        String typeName,
        GeneratedDdlStmt stmt)
    {
        createHeader(
            sb,
            typeName,
            stmt.isReplace(),
            stmt.getNewName());
    }

    protected void createHeader(
        SqlBuilder sb,
        String typeName,
        boolean replace,
        String newName)
    {
        sb.append("CREATE ");
        if (replace && typeSupportsReplace(typeName)) {
            sb.append("OR REPLACE ");
        }
        if (newName != null) {
            sb.append("RENAME TO ");
            sb.identifier(newName);
            sb.append(" ");
        }
        sb.append(typeName);
        sb.append(" ");
    }

    public void create(
        FemLocalView view,
        GeneratedDdlStmt stmt)
    {
        // Assume that the view was created in the context of its schema. If
        // that was not the case, we would have no way to detect it. We'd need
        // implement a closure mechanism to deal with SET SCHEMA and SET PATH.
        if (!uglyViews
              && generateSetSchema(stmt, view.getNamespace().getName(), false))
        {
            stmt.addStmt(";" + NL);
        }

        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "VIEW", stmt);
        name(sb, view.getNamespace(), view.getName());
        addDescription(sb, view);
        sb.append(" AS");
        sb.append(NL);
        stmt.addStmt(sb.getSqlAndClear());

        if (!uglyViews) {
            sb.append(view.getOriginalDefinition());
        } else {
            sb.append(view.getQueryExpression().getBody());
        }
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemLocalSchema schema,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "SCHEMA", stmt);

        name(sb, null, schema.getName());
        addDescription(sb, schema);

        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemUser user,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "USER", stmt);

        name(sb, null, user.getName());
        stmt.addStmt(sb.getSqlAndClear());

        sb.append(NL);
        if (user.getEncryptedPassword() == null) {
            sb.append("AUTHORIZATION ");
            // Auths are not stored by the parser.
            sb.literal("Unused");
        } else {
            sb.append("IDENTIFIED BY ");
            sb.literal("********");
        }
        stmt.addStmt(sb.getSqlAndClear());

        CwmNamespace ns = user.getDefaultNamespace();
        if (ns != null) {
            sb.append(NL);
            sb.append("DEFAULT ");
            if (ns instanceof FemLocalSchema) {
                sb.append("SCHEMA ");
            } else if (ns instanceof FemLocalCatalog) {
                sb.append("CATALOG ");
            }
            name(sb, ns.getNamespace(), ns.getName());
        }
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemRole role,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "ROLE", stmt);

        name(sb, null, role.getName());

        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemJar jar,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "JAR", stmt);

        name(sb, jar.getNamespace(), jar.getName());
        addDescription(sb, jar);

        stmt.addStmt(sb.getSqlAndClear());
        sb.append(NL);
        sb.append("LIBRARY ");
        sb.literal(jar.getUrl());
        sb.append(NL);
        sb.append("OPTIONS(");
        sb.append(jar.getDeploymentState());
        sb.append(")");

        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemLocalTable table,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "TABLE", false, null);

        name(sb, table.getNamespace(), table.getName());
        stmt.addStmt(sb.getSqlAndClear());

        addColumns(sb, table);
        addOptions(
            sb,
            table.getStorageOptions());
        addDescription(sb, table);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemForeignTable table,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "FOREIGN TABLE", false, null);

        name(sb, table.getNamespace(), table.getName());
        stmt.addStmt(sb.getSqlAndClear());

        addColumns(sb, table);
        sb.append(NL);
        sb.append("SERVER ");
        FemDataServer server = table.getServer();
        if (server != null) {
            name(sb, null, server.getName());
        }
        addOptions(
            sb,
            ((FemElementWithStorageOptions) table).getStorageOptions());
        addDescription(sb, table);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemDataWrapper wrapper,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "FOREIGN DATA WRAPPER", stmt);

        name(sb, null, wrapper.getName());
        stmt.addStmt(sb.getSqlAndClear());

        // "LIBRARY" clause is optional
        if (wrapper.getLibraryFile() != null) {
            sb.append(" LIBRARY ");
            sb.literal(wrapper.getLibraryFile());
        }
        sb.append(NL);
        sb.append("LANGUAGE ");
        sb.append(wrapper.getLanguage());
        addOptions(
            sb,
            wrapper.getStorageOptions());
        addDescription(sb, wrapper);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        CwmOperation operation,
        GeneratedDdlStmt stmt)
    {
        // We don't generate any DDL for an operation. Operations need to be
        // in the export set so that method implementations occur after type
        // declarations (which also declare operations).
        stmt.setTopLevel(false);
    }

    public void create(
        FemRoutine routine,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        final ProcedureType routineType = routine.getType();
        final CwmClassifier owner = routine.getSpecification().getOwner();
        boolean method =
            (routine.getSpecification() != null)
            && (owner
                instanceof FemUserDefinedType);
        if (method) {
            createHeader(sb, "SPECIFIC METHOD", stmt);
        } else if (routineType.equals(ProcedureTypeEnum.FUNCTION)) {
            createHeader(sb, "FUNCTION", stmt);
        } else if (routineType.equals(ProcedureTypeEnum.PROCEDURE)) {
            createHeader(sb, "PROCEDURE", stmt);
        }

        name(sb, routine.getNamespace(), routine.getName());
        stmt.addStmt(sb.getSqlAndClear());

        if (method) {
            sb.append(NL);
            sb.append("FOR ");
            name(sb, owner.getNamespace(), owner.getName());
        } else {
            methodBody(routine, sb);
        }

        if (routine.getExternalName() == null) {
            sb.append(NL);
            sb.append(routine.getBody().getBody());
        }

        stmt.addStmt(sb.getSqlAndClear());
    }

    /**
     * Generates a parameters, keyword list and body of a method.
     *
     * @param routine Routine
     * @param sb String buffer to write to
     */
    private void methodBody(
        FemRoutine routine,
        SqlBuilder sb)
    {
        final ProcedureType routineType = routine.getType();
        boolean method =
            (routine.getSpecification() != null)
            && (routine.getSpecification().getOwner()
                instanceof FemUserDefinedType);

        sb.append("(");
        sb.append(NL);
        List<CwmParameter> returns = new ArrayList<CwmParameter>();
        int paramCount = 0;
        for (CwmParameter parameter : routine.getParameter()) {
            if (parameter.getKind().equals(
                    ParameterDirectionKindEnum.PDK_RETURN))
            {
                assert !routineType.equals(ProcedureTypeEnum.PROCEDURE);
                returns.add(parameter);
                continue;
            }
            if (paramCount++ > 0) {
                sb.append(",");
                sb.append(NL);
            }
            add((FemRoutineParameter) parameter, routineType, sb);
        }
        sb.append(")");
        sb.append(NL);
        for (CwmParameter aReturn : returns) {
            add((FemRoutineParameter) aReturn, routineType, sb);
            sb.append(NL);
        }

        if (method) {
            sb.append("SELF AS RESULT");
            sb.append(NL);
        }

        sb.append("SPECIFIC ");
        name(sb, routine.getNamespace(), routine.getName());

        sb.append(NL);
        sb.append("LANGUAGE ");
        sb.append(routine.getLanguage());

        final RoutineDataAccess da = routine.getDataAccess();
        if (da.equals(RoutineDataAccessEnum.RDA_MODIFIES_SQL_DATA)) {
            sb.append(NL).append("MODIFIES SQL DATA");
        } else if (da.equals(RoutineDataAccessEnum.RDA_CONTAINS_SQL)) {
            sb.append(NL).append("CONTAINS SQL");
        } else if (da.equals(RoutineDataAccessEnum.RDA_NO_SQL)) {
            sb.append(NL).append("NO SQL");
        } else if (da.equals(RoutineDataAccessEnum.RDA_READS_SQL_DATA)) {
            sb.append(NL).append("READS SQL DATA");
        }

        sb.append(NL);
        sb.append(maybeNot(routine.isDeterministic(), "DETERMINISTIC"));

        // Elide STATIC DISPATCH for functions and procedures, where it is
        // the only option.
        if (routine.isStaticDispatch() && method) {
            sb.append(NL);
            sb.append("STATIC DISPATCH");
        }

        if (routine.getExternalName() != null) {
            sb.append(NL);
            sb.append("EXTERNAL NAME ");
            sb.literal(routine.getExternalName());
        }
    }

    /**
     * Generates DDL for a routine parameter.
     *
     * @param parameter Parameter
     * @param routineType Type of routine parameter belongs to
     * @param sb StringBuilder to append to
     */
    private void add(
        FemRoutineParameter parameter,
        ProcedureType routineType,
        SqlBuilder sb)
    {
        final ParameterDirectionKind kind = parameter.getKind();
        boolean qualifyType = false;
        if (kind.equals(ParameterDirectionKindEnum.PDK_IN)) {
            if (routineType.equals(ProcedureTypeEnum.PROCEDURE)) {
                sb.append("  IN ");
            } else {
                sb.append("  ");
            }
            sb.identifier(parameter.getName());
        } else if (kind.equals(ParameterDirectionKindEnum.PDK_INOUT)) {
            sb.append("  INOUT ");
            sb.identifier(parameter.getName());
        } else if (kind.equals(ParameterDirectionKindEnum.PDK_OUT)) {
            sb.append("  OUT ");
            sb.identifier(parameter.getName());
        } else if (kind.equals(ParameterDirectionKindEnum.PDK_RETURN)) {
            qualifyType = true;
            sb.append("RETURNS");
        }

        // REVIEW: functions don't have OUT or INOUT params
        // REVIEW: procedures don't have RETURNS

        sb.append(" ");
        appendType(
            sb,
            parameter.getType(),
            parameter.getPrecision(),
            parameter.getScale(),
            parameter.getLength(),
            null,
            parameter.getDefaultValue(),
            qualifyType);
    }

    private void appendType(
        SqlBuilder sb,
        CwmClassifier type,
        Integer precision,
        Integer scale,
        Integer length,
        NullableType nullable,
        CwmExpression defaultValue,
        boolean qualifyType)
    {
        if ((type instanceof FemSqlobjectType) && qualifyType) {
            // Workaround FRG-297; remove qualifyType parameter when fixed
            sb.append(type.getNamespace().getName());
            sb.append('.');
        }
        formatTypeInfo(sb, type, precision, scale, length);

        if (defaultValue != null) {
            String val = defaultValue.getBody();
            if ((val != null) && !val.equals(VALUE_NULL)) {
                sb.append(" DEFAULT ");

                // we expect the setter of body to be responsible
                // for forming a valid SQL expression given the
                // datatype
                sb.append(val);
            }
        }

        if (nullable != null) {
            if (NullableTypeEnum.COLUMN_NO_NULLS.toString().equals(
                    nullable.toString()))
            {
                sb.append(" NOT NULL");
            }
        }
    }

    /**
     * Format the core elements of a column's type (type name, precision, scale,
     * length) into SQL format.
     *
     * @param sb SQL builder
     * @param col CwmColumn object we want type info for
     */
    public static void formatTypeInfo(
        SqlBuilder sb,
        CwmColumn col)
    {
        formatTypeInfo(
            sb,
            col.getType(),
            col.getPrecision(),
            col.getScale(),
            col.getLength());
    }

    /**
     * Format the core elements of a column's type (type name, precision, scale,
     * length) into SQL format.
     *
     * <p>Note that this was refactored out of {@link #appendType} to allow
     * separate access.
     *
     * @param sb StringBuilder to hold the formatted type information
     * @param type CwmClassifier object representing the column type
     * @param precision Integer specifying the column's precision
     * @param scale Integer specifying the column's scale
     * @param length Integer specifying the column's length
     */
    public static void formatTypeInfo(
        SqlBuilder sb,
        CwmClassifier type,
        Integer precision,
        Integer scale,
        Integer length)
    {
        sb.append(type.getName());

        SqlTypeName stn = getSqlTypeName(type);
        if ((precision != null) && stn.allowsPrec()) {
            sb.append("(").append(precision);
            if ((scale != null) && stn.allowsScale()) {
                sb.append(",").append(scale);
            }
            sb.append(")");
        } else {
            if (length != null) {
                sb.append("(").append(length).append(")");
            }
        }
    }

    /**
     * Generates either "s" or "NOT s", depending on <code>b</code>.
     *
     * @param b Condition
     * @param s Flag string
     *
     * @return string containing s or not s
     */
    protected static String maybeNot(boolean b, String s)
    {
        return b ? s : ("NOT " + s);
    }

    public void create(
        FemSqlobjectType type,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "TYPE", stmt);
        name(sb, type.getNamespace(), type.getName());
        sb.append(" AS");
        addColumns(sb, type);
        sb.append(NL);
        sb.append(maybeNot(type.isFinal(), "FINAL"));
        sb.append(NL);
        sb.append(maybeNot(!type.isAbstract(), "INSTANTIABLE"));
        addDescription(sb, type);

        addOperations(sb, Util.filter(type.getFeature(), CwmOperation.class));
        stmt.addStmt(sb.getSqlAndClear());
    }

    private void addOperations(
        SqlBuilder sb,
        List<CwmOperation> operations)
    {
        for (CwmOperation operation : operations) {
            sb.append(NL);
            sb.append("CONSTRUCTOR METHOD ");
            sb.identifier(operation.getName());
            sb.append(" ");

            // REVIEW: I think there is precisely one method per operation
            for (CwmMethod method : operation.getMethod()) {
                methodBody((FemRoutine) method, sb);
            }
        }
    }

    public void create(
        FemSqldistinguishedType type,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "TYPE", stmt);
        name(sb, type.getNamespace(), type.getName());
        sb.append(" AS ");

        appendType(
            sb,
            type.getType(),
            type.getPrecision(),
            type.getScale(),
            type.getLength(),
            null,
            null,
            true);

        sb.append(NL);
        sb.append(maybeNot(type.isFinal(), "FINAL"));
        sb.append(NL);
        sb.append(maybeNot(!type.isAbstract(), "INSTANTIABLE"));
        addDescription(sb, type);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        FemDataServer server,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "SERVER", stmt);

        name(sb, null, server.getName());
        stmt.addStmt(sb.getSqlAndClear());

        // "TYPE" clause is optional
        final String type = server.getType();
        if ((type != null) && !type.equals("UNKNOWN")) {
            sb.append(" TYPE ");
            sb.literal(type);
        }

        // "VERSION" clause is optional
        final String version = server.getVersion();
        if ((version != null) && !version.equals("UNKNOWN")) {
            sb.append(" VERSION ");
            sb.literal(version);
        }
        sb.append(NL);
        sb.append("FOREIGN DATA WRAPPER ");
        name(sb, null, server.getWrapper().getName());
        addOptions(
            sb,
            server.getStorageOptions());
        addDescription(sb, server);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void create(
        CwmDependency dependency,
        GeneratedDdlStmt stmt)
    {
        // We don't generate any DDL for a dependency. Dependencies need to be
        // in the export set to ensure that objects occur in the right order.
        stmt.setTopLevel(false);
    }

    public void drop(
        CwmSchema schema,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        sb.append("DROP SCHEMA ");
        name(sb, null, schema.getName());
        if (dropCascade) {
            sb.append(" CASCADE");
        }
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void drop(
        FemRole role,
        GeneratedDdlStmt stmt)
    {
        drop(role, "ROLE", stmt);
    }

    public void drop(
        FemUser user,
        GeneratedDdlStmt stmt)
    {
        drop(user, "USER", stmt);
    }

    public void drop(
        FemJar jar,
        GeneratedDdlStmt stmt)
    {
        drop(jar, "JAR", stmt);
    }

    public void drop(
        CwmView view,
        GeneratedDdlStmt stmt)
    {
        drop(view, "VIEW", stmt);
    }

    public void drop(
        FemDataServer server,
        GeneratedDdlStmt stmt)
    {
        drop(server, "SERVER", stmt);
    }

    public void drop(
        CwmTable table,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        sb.append("DROP TABLE ");
        name(sb, table.getNamespace(), table.getName());
        if (dropCascade) {
            sb.append(" CASCADE");
        }
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void drop(
        FemDataWrapper plugin,
        GeneratedDdlStmt stmt)
    {
        drop(plugin, "FOREIGN DATA WRAPPER", stmt);
    }

    public void create(
        FemLocalIndex index,
        GeneratedDdlStmt stmt)
    {
        if (index.isClustered()
            || index.getName().startsWith("SYS$CONSTRAINT_INDEX$")
            || index.getName().startsWith("SYS$DELETION_INDEX"))
        {
            stmt.setTopLevel(false);
        }
        SqlBuilder sb = createSqlBuilder();
        createHeader(
            sb,
            index.isClustered() ? "CLUSTERED INDEX" : "INDEX",
            stmt);
        name(sb, null, index.getName());
        sb.append(" ON ");
        final CwmClass spanned = index.getSpannedClass();
        name(sb, spanned.getNamespace(), spanned.getName());
        sb.append(" (");
        int k = -1;
        for (CwmIndexedFeature feature : index.getIndexedFeature()) {
            if (++k > 0) {
                sb.append(", ");
            }
            sb.identifier(feature.getName());
        }
        sb.append(")");
        addDescription(sb, index);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void drop(
        FemLocalIndex index,
        GeneratedDdlStmt stmt)
    {
        drop(index, "INDEX", stmt);
    }

    public void create(
        FemLabel label,
        GeneratedDdlStmt stmt)
    {
        SqlBuilder sb = createSqlBuilder();
        createHeader(sb, "LABEL", stmt);
        name(sb, null, label.getName());
        if (label.getParentLabel() != null) {
            sb.append(" FROM LABEL ");
            name(sb, null, label.getParentLabel().getName());
        }
        addDescription(sb, label);
        stmt.addStmt(sb.getSqlAndClear());
    }

    public void drop(
        FemLabel label,
        GeneratedDdlStmt stmt)
    {
        drop(label, "LABEL", stmt);
    }

    protected void addColumns(
        SqlBuilder sb,
        CwmClassifier table)
    {
        addColumns(sb, table, false, false);
    }

    protected void addColumns(
        SqlBuilder sb,
        CwmClassifier table,
        boolean skipDefaults,
        boolean skipNullable)
    {
        generateColumnsAndKeys(sb, table, skipDefaults, skipNullable, null);
    }

    /**
     * Generates the column and key definitions of a table as a SQL string
     * (enclosed in parentheses unless there are no columns).
     *
     * @param sb receives generated string
     * @param table table to iterate down for columns and keys
     * @param skipDefaults whether to omit default value definitions
     * @param skipNullable whether to omit NOT NULL constraint definitions
     * @param imposedPrimaryKey if not null, use as PRIMARY KEY
     */
    public void generateColumnsAndKeys(
        SqlBuilder sb,
        CwmClassifier table,
        boolean skipDefaults,
        boolean skipNullable,
        List<String> imposedPrimaryKey)
    {
        boolean isLast = false;
        List<String> pk = imposedPrimaryKey;

        List<CwmColumn> columns =
            Util.filter(table.getFeature(), CwmColumn.class);

        if (columns.size() > 0) {
            sb.append(" (");
            sb.append(NL);
            final Iterator<CwmColumn> columnIter = columns.iterator();
            while (columnIter.hasNext()) {
                CwmColumn col = columnIter.next();

                if (imposedPrimaryKey == null) {
                    if (col instanceof FemStoredColumn) {
                        if (hasPrimaryKeyConstraint((FemStoredColumn) col)) {
                            if (pk == null) {
                                pk = new ArrayList<String>();
                            }
                            pk.add(col.getName());
                        }
                    }
                }

                isLast = !columnIter.hasNext() && (pk == null);

                sb.append("   ").identifier(col.getName());

                sb.append(" ");
                CwmExpression e;
                if (skipDefaults) {
                    e = null;
                } else {
                    e = col.getInitialValue();
                }
                final NullableType isNullable;
                if (skipNullable) {
                    isNullable = null;
                } else {
                    isNullable = col.getIsNullable();
                }
                appendType(
                    sb,
                    col.getType(),
                    col.getPrecision(),
                    col.getScale(),
                    col.getLength(),
                    isNullable,
                    e,
                    true);

                // is this a stored column?
                if (col instanceof FemElementWithStorageOptions) {
                    addOptions(
                        sb,
                        ((FemElementWithStorageOptions) col)
                        .getStorageOptions(),
                        2);
                }

                if (!isLast) {
                    sb.append(",");
                }

                sb.append(NL);
            }

            addPrimaryKeyConstraint(sb, pk);
            // add unique constraints
            List<FemUniqueKeyConstraint> uniques = getUniqueKeyConstraints(
                    table);
            // sort alphabetically
            Collections.sort(
                    uniques, new Comparator<Object>() {
                        public int compare(Object obj1, Object obj2) {
                            return (
                              (FemUniqueKeyConstraint)obj1).getName().compareTo(
                                  ((FemUniqueKeyConstraint)obj2).getName());
                        }
                    });

            boolean firstConst = true;
            for (FemUniqueKeyConstraint constraint : uniques) {
                if (firstConst) {
                    if (pk.size() > 0) {
                        sb.append(",");
                        sb.append(NL);
                    }
                    firstConst = false;
                } else {
                    sb.append(",");
                    sb.append(NL);
                }
                sb.append("   CONSTRAINT ");
                sb.identifier(constraint.getName());
                sb.append(" UNIQUE(");
                List<CwmColumn> cols = Util.filter(
                        constraint.getFeature(), CwmColumn.class);
                boolean firstCol = true;
                for (CwmColumn col : cols) {
                    if (!firstCol) {
                        sb.append(",");
                    } else {
                        firstCol = false;
                    }
                    sb.identifier(col.getName());
                }
                sb.append(")");
            }

            sb.append(NL);
            sb.append(")");
        }
    }

    // NOTE: Copied from FarragoCatalogUtil
    private List<FemUniqueKeyConstraint> getUniqueKeyConstraints(
        CwmClassifier table)
    {
        List<FemUniqueKeyConstraint> listOfConstraints =
            new ArrayList<FemUniqueKeyConstraint>();

        for (Object obj : table.getOwnedElement()) {
            if (obj instanceof FemUniqueKeyConstraint) {
                listOfConstraints.add((FemUniqueKeyConstraint) obj);
            }
        }
        return listOfConstraints;
    }

    protected void addOptions(
        SqlBuilder sb,
        Collection<FemStorageOption> options)
    {
        addOptions(sb, options, 1);
    }

    protected void addOptions(
        SqlBuilder sb,
        Collection<FemStorageOption> options,
        int indent)
    {
        if ((options == null) || (options.size() == 0)) {
            return;
        }

        List<FemStorageOption> sortedOptions =
            new ArrayList<FemStorageOption>(options);
        Collections.sort(sortedOptions, new FemStorageOptionNameComparator());

        if (indent == 1) {
            sb.append(NL);
        }
        sb.append("OPTIONS (");
        sb.append(NL);
        int k = 0;
        for (FemStorageOption option : sortedOptions) {
            indent(sb, indent * 2);
            sb.identifier(option.getName());
            sb.append(" ");
            sb.literal(option.getValue());
            if (++k < sortedOptions.size()) {
                sb.append(",");
            }
            sb.append(NL);
        }
        sb.append(")");
    }

    private static void indent(SqlBuilder sb, int indent)
    {
        for (int j = 0; j < indent; j++) {
            sb.append(' ');
        }
    }

    private void addPrimaryKeyConstraint(
        SqlBuilder sb,
        List<String> keyColumns)
    {
        if (keyColumns != null) {
            sb.append("   PRIMARY KEY (");
            boolean isFirst = true;
            for (String keyColumn : keyColumns) {
                if (!isFirst) {
                    sb.append(",");
                } else {
                    isFirst = false;
                }
                sb.identifier(keyColumn);
            }
            sb.append(")");
        }
    }

    protected void addDescription(
        SqlBuilder sb,
        FemAnnotatedElement element)
    {
        String desc = element.getDescription();
        if ((desc != null) && (desc.length() > 0)) {
            sb.append(NL);
            sb.append("DESCRIPTION ");
            sb.literal(desc);
        }
    }

    protected void drop(
        CwmModelElement e,
        String elementType,
        GeneratedDdlStmt stmt)
    {
        if (e == null) {
            return;
        }
        SqlBuilder sb = createSqlBuilder();
        sb.append("DROP ").append(elementType).append(" ");

        name(sb, e.getNamespace(), e.getName());
        if (dropCascade) {
            sb.append(" CASCADE");
        }
        stmt.addStmt(sb.getSqlAndClear());
    }

    //~ Inner Classes ----------------------------------------------------------

    protected static class FemStorageOptionNameComparator
        implements Comparator<FemStorageOption>
    {
        public int compare(FemStorageOption o1, FemStorageOption o2)
        {
            int c = o1.getName().compareTo(o2.getName());

            if (c != 0) {
                return c;
            }

            return o1.getValue().compareTo(o2.getValue());
        }
    }
}

// End FarragoDdlGenerator.java
