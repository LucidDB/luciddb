/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.session;

import java.util.*;
import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * FarragoSessionStmtValidator defines a generic interface for statement
 * validation services.  It is not as specific as the other validator-related
 * interfaces ({@link FarragoSessionDdlValidator} and {@link
 * FarragoSessionPreparingStmt}).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionStmtValidator extends FarragoAllocationOwner
{
    //~ Methods ---------------------------------------------------------------

    /**
     * @return session invoking stmt to be validated
     */
    public FarragoSession getSession();

    /**
     * @return the parser parsing the statement being validated
     */
    public FarragoSessionParser getParser();

    /**
     * @return repos to use for validating object references
     */
    public FarragoRepos getRepos();

    /**
     * @return FennelDbHandle storing local data to be accessed by
     * validated stmt
     */
    public FennelDbHandle getFennelDbHandle();

    /**
     * @return type factory to use for validation
     */
    public FarragoTypeFactory getTypeFactory();

    /**
     * @return connection defaults to use for validation
     */
    public FarragoSessionVariables getSessionVariables();

    /**
     * @return cache to use for code lookups during validation
     */
    public FarragoObjectCache getCodeCache();

    /**
     * @return private cache to use for validating references to
     * data wrappers
     */
    public FarragoDataWrapperCache getDataWrapperCache();

    /**
     * @return index map to use for validation
     */
    public FarragoSessionIndexMap getIndexMap();

    /**
     * @return shared cache to use for validating references to
     * data wrappers
     */
    public FarragoObjectCache getSharedDataWrapperCache();

    /**
     * Looks up a table's column by name, throwing a validation error if not
     * found.
     *
     * @param table the table to search
     * @param columnName name of column to find
     *
     * @return column found
     */
    public CwmColumn findColumn(
        CwmNamedColumnSet namedColumnSet,
        String columnName);

    /**
     * Looks up a catalog by name, throwing a validation error if not found.
     *
     * @param catalogName name of catalog to look up
     *
     * @return catalog found
     */
    public CwmCatalog findCatalog(String catalogName);

    /**
     * Gets the default catalog for unqualified schema names.
     *
     * @return default catalog
     */
    public CwmCatalog getDefaultCatalog();

    /**
     * Looks up a schema by name, throwing a validation error if not found.
     *
     * @param schemaName name of schema to look up
     *
     * @return schema found
     */
    public FemLocalSchema findSchema(SqlIdentifier schemaName);

    /**
     * Looks up a data wrapper by name, throwing a validation error if not
     * found.
     *
     * @param wrapperName name of wrapper to look up (must be simple)
     *
     * @param isForeign true for foreign data wrapper; false for
     * local data wrapper
     *
     * @return wrapper found
     */
    public FemDataWrapper findDataWrapper(
        SqlIdentifier wrapperName,
        boolean isForeign);

    /**
     * Looks up a data server by name, throwing a validation error if not found.
     *
     * @param serverName name of server to look up (must be simple)
     *
     * @return server found
     */
    public FemDataServer findDataServer(SqlIdentifier serverName);

    /**
     * @return default data server to use if none specified in
     * local table definition
     */
    public FemDataServer getDefaultLocalDataServer();

    /**
     * Looks up a schema object by name, throwing a validation error if not
     * found.
     *
     * @param qualifiedName name of object to look up
     *
     * @param refClass expected class of object; if the object exists with a
     *        different class, it will be treated as if it did not exist
     *
     * @return schema object found
     */
    public CwmModelElement findSchemaObject(
        SqlIdentifier qualifiedName,
        RefClass refClass);

    /**
     * Looks up all matching routine overloads by invocation name.
     *
     * @param invocationName invocation name of routine to look up
     *
     * @param routineType type of routine to look up, or null
     * for any type
     *
     * @return list of matching FemRoutine objects (empty if no matches)
     */
    public List findRoutineOverloads(
        SqlIdentifier invocationName,
        ProcedureType routineType);

    /**
     * Looks up a SQL datatype by name, throwing an exception if not found.
     *
     * @param typeName name of type to find
     *
     * @return type definition
     */
    public CwmSqldataType findSqldataType(SqlIdentifier typeName);

    /**
     * Resolve a (possibly qualified) name of a schema object.
     *
     * @param names array of 1 or more name components, from
     * most general to most specific
     *
     * @param refClass type of object to resolve
     *
     * @return FarragoSessionResolvedObject, or null if object definitely
     * doesn't exist
     */
    public FarragoSessionResolvedObject resolveSchemaObjectName(
        String [] names,
        RefClass refClass);

    /**
      * Gets schema object names as specified. They can be schema or table
      * object.
      * If names array contain 1 element, return all schema names and 
      *    all table names under the default schema (if that is set)
      * If names array contain 2 elements, treat 1st element as schema name
      *    and return all table names in this schema
      *
      * @param names the array contains either 2 elements representing a 
      * partially qualified object name in the format of 'schema.object', or an 
      * unqualified name in the format of 'object'
      *
      * @return the list of all object (schema and table) names under the above
      *  criteria
      */
    public String [] getAllSchemaObjectNames(String [] names);

    /**
     * Sets the parser position to use for context in error messages.
     *
     * @param pos new position to set, or null to clear
     */
    public void setParserPosition(SqlParserPos pos);
}


// End FarragoSessionStmtValidator.java
