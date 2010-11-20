/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/

package net.sf.farrago.syslib;

import java.sql.*;
import java.util.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.gen.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.*;

/**
 * Implements several methods to select the DDL statements for most types
 * of objects.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class FarragoDdlViewUDR
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Generates a CREATE OR REPLACE DDL for the schema and everything
     * it contains.
     *
     * @param schema_name - Schema name for ddl creation.
     * @param resultInserter - Handles the output.
     */
    public static void generateForSchema(
            String schema_name,
            PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(resultInserter, getElements(schema_name));
    }

    /**
     * Generates a DDL for some object in a schema identified by
     * its name. In the case of name collision, all items with the
     * given name will be inserted into the output.
     *
     * @param schema_name - Schema name for ddl creation.
     * @param element_name - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForObject(
            String schema_name,
            String element_name,
            PreparedStatement resultInserter)
        throws SQLException
    {
        List<CwmModelElement> el_list = getElements(schema_name);
        List<CwmModelElement> filtered_els = new ArrayList<CwmModelElement>();

        for (CwmModelElement element : el_list) {
            if (element.getName().equals(element_name)) {
                filtered_els.add(element);
            }
        }

        createDdl(resultInserter, filtered_els);
    }

    /**
     * Generates DDL for every available item in the entire current catalog.
     *
     * @param resultInserter - Handles the output.
     */
    public static void generateForCatalog(PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(resultInserter, getAllElements());

    }

    /**
     * Generates DDL for a specific table- or view-name in a schema.
     *
     * @param schema_name - Schema name for ddl creation.
     * @param table_name - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForTable(
            String schema_name,
            String table_name,
            PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(
                resultInserter,
                getElementByNameAndType(
                    schema_name, table_name, FemAbstractColumnSet.class));
    }

    /**
     * Generates DDL for a specific procedure- or function-name in a schema.
     *
     * @param schema_name - Schema name for ddl creation.
     * @param routine_name - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForRoutine(
            String schema_name,
            String routine_name,
            PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(
                resultInserter,
                getElementByNameAndType(
                    schema_name, routine_name, FemRoutine.class));
    }

    /**
     * Generates DDL for a specific jar-name in a schema.
     *
     * @param schema_name - Schema name for ddl creation.
     * @param jar_name - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForJar(
            String schema_name,
            String jar_name,
            PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(
                resultInserter,
                getElementByNameAndType(
                    schema_name, jar_name, FemJar.class));
    }

    /**
     * @return Returns a list of all catalog elements.
     */
    private static List<CwmModelElement> getAllElements()
    {
        FarragoDdlGenerator gen = getGenerator();
        CwmCatalog catalog = getCatalog();
        List<CwmModelElement> el_list = new ArrayList<CwmModelElement>();
        gen.gatherElements(el_list, null, true, catalog);
        return el_list;
    }

    /**
     * @param schema_name - Schema to limit element list.
     * @return Returns a list of all elements in a schema.
     */
    private static List<CwmModelElement> getElements(String schema_name)
    {
        FarragoDdlGenerator gen = getGenerator();
        CwmCatalog catalog = getCatalog();
        List<CwmModelElement> el_list = new ArrayList<CwmModelElement>();
        gen.gatherElements(el_list, schema_name, false, catalog);
        return el_list;
    }

    /**
     * @param schema_name - Schema to limit element list.
     * @param element_name - Element name to limit element list.
     * @param class_type - Final filter on element's type.
     * @return Returns a list of elements filtered by schema, element name,
     * and element type.
     */
    private static <T extends CwmModelElement> List<CwmModelElement>
        getElementByNameAndType(
            String schema_name,
            String element_name,
            Class<T> class_type)
    {
        List<CwmModelElement> el_list = new ArrayList<CwmModelElement>();
        el_list.add(FarragoCatalogUtil.getModelElementByNameAndType(
                    getElements(schema_name), element_name, class_type));
        el_list.remove(null);
        return el_list;
    }

    /**
     * Computes and writes the DDL to the resultInserter output.
     *
     * @param resultInserter - Handles output.
     * @param el_list - List of elements for which to generate DDL statements.
     */
    private static void createDdl(
            PreparedStatement resultInserter,
            List<CwmModelElement> el_list)
        throws SQLException
    {
        FarragoDdlGenerator gen = getGenerator();
        String statement = "";
        if (el_list.size() > 0) {
            statement = gen.getExportText(el_list, true);
        }
        resultInserter.setString(1, statement);
        resultInserter.executeUpdate();
    }

    /**
     * @return Returns a new DdlGenerator object for the purpose of getting
     * elements and generating DDL statements.
     */
    private static FarragoDdlGenerator getGenerator()
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoRepos repos = FarragoUdrRuntime.getRepos();
        FarragoDdlGenerator gen = new FarragoDdlGenerator(
                SqlDialect.EIGENBASE,
                repos.getModelView());
        gen.setSchemaQualified(true);
        return gen;
    }

    /**
     * @return Returns the current session's active catalog for the purpose
     * of finding its contained elements.
     */
    private static CwmCatalog getCatalog()
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoRepos repos = FarragoUdrRuntime.getRepos();
        CwmCatalog cat = repos.getCatalog(
                session.getSessionVariables().catalogName);
        return cat;
    }

}

// End FarragoDdlViewUDR.java
