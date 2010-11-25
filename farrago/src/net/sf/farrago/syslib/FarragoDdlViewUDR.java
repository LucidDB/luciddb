/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 DynamoBI Corporation
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
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;

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

    //~ Static fields/initializers ---------------------------------------------

    private static String catalog;

    //~ Methods ----------------------------------------------------------------

    /**
     * Generates a CREATE OR REPLACE DDL for the schema and everything
     * it contains.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param resultInserter - Handles the output.
     */
    public static void generateForSchema(
            String schemaName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        createDdl(resultInserter, getElements(schemaName));
    }

    public static void generateForSchema(
            String catalogName,
            String schemaName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForSchema(schemaName, resultInserter);
    }

    /**
     * Generates a DDL for some object in a schema identified by
     * its name. In the case of name collision, all items with the
     * given name will be inserted into the output.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param elementName - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForObject(
            String schemaName,
            String elementName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        List<CwmModelElement> elList = getElements(schemaName);
        List<CwmModelElement> filteredEls = new ArrayList<CwmModelElement>();

        for (CwmModelElement element : elList) {
            if (element.getName().equals(elementName)) {
                filteredEls.add(element);
            }
        }

        createDdl(resultInserter, filteredEls);
    }

    public static void generateForObject(
            String catalogName,
            String schemaName,
            String elementName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForObject(schemaName, elementName, resultInserter);
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

    public static void generateForCatalog(
            String catalogName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForCatalog(resultInserter);
    }

    /**
     * Generates DDL for a specific table- or view-name in a schema.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param table - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForTable(
            String schemaName,
            String table,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(schemaName, table, resultInserter, FemAbstractColumnSet.class);
    }

    public static void generateForTable(
            String catalogName,
            String schemaName,
            String tableName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForTable(schemaName, tableName, resultInserter);
    }

    /**
     * Generates DDL for a procedure or function name, based on the invocation
     * name, returning all matches.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param routineName - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForRoutine(
            String schemaName,
            String routineName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        List<CwmModelElement> routines = new ArrayList<CwmModelElement>();

        for (CwmModelElement el : getElements(schemaName)) {
            if (el instanceof FemRoutine) {
                FemRoutine routine = (FemRoutine) el;
                if (routine.getInvocationName().equals(routineName)) {
                    routines.add(el);
                }
            }
        }
        createDdl(resultInserter, routines);
    }

    public static void generateForRoutine(
            String catalogName,
            String schemaName,
            String routineName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForRoutine(schemaName, routineName, resultInserter);
    }

    /**
     * Finds routines based on specific names rather than invocation name.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param routineName - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForSpecificRoutine(
            String schemaName,
            String routineName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(schemaName, routineName, resultInserter, FemRoutine.class);
    }

    public static void generateForSpecificRoutine(
            String catalogName,
            String schemaName,
            String routineName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForSpecificRoutine(schemaName, routineName, resultInserter);
    }


    /**
     * Generates DDL for a specific jar-name in a schema.
     *
     * @param schemaName - Schema name for ddl creation.
     * @param jarName - Element in schema to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForJar(
            String schemaName,
            String jarName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(schemaName, jarName, resultInserter, FemJar.class);
    }

    public static void generateForJar(
            String catalogName,
            String schemaName,
            String jarName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForJar(schemaName, jarName, resultInserter);
    }

    /**
     * Generates DDL for a specific foreign server name.
     *
     * @param serverName - Server to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForServer(
            String serverName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(null, serverName, resultInserter, FemDataServer.class);
    }

    /**
     * Generates DDL for a specific foreign wrapper name.
     *
     * @param wrapperName - Wrapper to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForWrapper(
            String wrapperName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(null, wrapperName, resultInserter, FemDataWrapper.class);
    }

    /**
     * Generates DDL for a specific index name.
     *
     * @param indexName - Index to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForIndex(
            String schemaName,
            String indexName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(schemaName, indexName, resultInserter, FemLocalIndex.class);
    }

    public static void generateForIndex(
            String catalogName,
            String schemaName,
            String indexName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        catalog = catalogName;
        generateForIndex(schemaName, indexName, resultInserter);
    }

    /**
     * Generates DDL for a specific user name.
     *
     * @param userName - User to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForUser(
            String userName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(null, userName, resultInserter, FemUser.class);
    }

    /**
     * Generates DDL for a specific role name.
     *
     * @param roleName - Role to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForRole(
            String roleName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(null, roleName, resultInserter, FemRole.class);
    }

    /**
     * Generates DDL for a specific label name.
     *
     * @param labelName - Label to search for.
     * @param resultInserter - Handles the output.
     */
    public static void generateForLabel(
            String labelName,
            PreparedStatement resultInserter)
        throws SQLException
    {
        generate(null, labelName, resultInserter, FemLabel.class);
    }

    /**
     * Generic method that actually performs the Ddl creation on a given
     * element.
     */
    private static <T extends CwmModelElement> void generate(
            String schemaName,
            String elementName,
            PreparedStatement resultInserter,
            Class<T> clazz)
        throws SQLException
    {
        createDdl(
                resultInserter,
                getElementByNameAndType(schemaName, elementName, clazz));
    }

    /**
     * @return Returns a list of all catalog elements.
     */
    private static List<CwmModelElement> getAllElements()
    {
        FarragoDdlGenerator gen = getGenerator();
        CwmCatalog catalog = getCatalog();
        List<CwmModelElement> elList = new ArrayList<CwmModelElement>();
        gen.gatherElements(elList, null, true, catalog);
        return elList;
    }

    /**
     * @param schemaName - Schema to limit element list.
     * @return Returns a list of all elements in a schema.
     */
    private static List<CwmModelElement> getElements(String schemaName)
    {
        FarragoDdlGenerator gen = getGenerator();
        CwmCatalog catalog = getCatalog();
        List<CwmModelElement> elList = new ArrayList<CwmModelElement>();
        gen.gatherElements(elList, schemaName, false, catalog);
        return elList;
    }

    /**
     * @param schemaName - Schema to limit element list.
     * @param elementName - Element name to limit element list.
     * @param classType - Final filter on element's type.
     * @return Returns a list of elements filtered by schema, element name,
     * and element type.
     */
    private static <T extends CwmModelElement> List<CwmModelElement>
        getElementByNameAndType(
            String schemaName,
            String elementName,
            Class<T> classType)
    {
        List<CwmModelElement> elList = new ArrayList<CwmModelElement>();
        List<CwmModelElement> checkedEls;
        if (schemaName == null) {
            checkedEls = getAllElements();
        } else {
            checkedEls = getElements(schemaName);
        }
        CwmModelElement el = FarragoCatalogUtil.getModelElementByNameAndType(
                checkedEls, elementName, classType);
        if (el != null) {
            elList.add(el);
        }
        return elList;
    }

    /**
     * Computes and writes the DDL to the resultInserter output.
     *
     * @param resultInserter - Handles output.
     * @param elList - List of elements for which to generate DDL statements.
     */
    private static void createDdl(
            PreparedStatement resultInserter,
            List<CwmModelElement> elList)
        throws SQLException
    {
        FarragoDdlGenerator gen = getGenerator();
        String statement = "";
        if (elList.size() > 0) {
            statement = gen.getExportText(elList, true);
        }
        for (String line : statement.split("\n")) {
            resultInserter.setString(1, line);
            resultInserter.executeUpdate();
        }
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
        gen.setUglyViews(true);
        return gen;
    }

    /**
     * @return Returns the current session's active catalog for the purpose
     * of finding its contained elements.
     */
    private static CwmCatalog getCatalog()
    {
        FarragoRepos repos = FarragoUdrRuntime.getRepos();
        CwmCatalog cat;
        if (catalog == null) {
            FarragoSession session = FarragoUdrRuntime.getSession();
            cat = repos.getCatalog(session.getSessionVariables().catalogName);
        } else {
            cat = repos.getCatalog(catalog);
            // Always assume default cat unless explicitly overridden.
            catalog = null;
        }
        assert (cat != null) : "Specified catalog does not exist.";
        return cat;
    }

}

// End FarragoDdlViewUDR.java
