/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.session;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorException;
import org.eigenbase.sql.parser.*;


/**
 * FarragoSessionDdlValidator represents an object capable of validating
 * a DDL statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionDdlValidator extends FarragoAllocation
{
    //~ Methods ---------------------------------------------------------------

    /**
     * @return generic stmt validator
     */
    public FarragoSessionStmtValidator getStmtValidator();

    /**
     * @return repos storing object definitions being validated
     */
    public FarragoRepos getRepos();

    /**
     * @return Handle to Fennel database accessed by this stmt
     */
    public FennelDbHandle getFennelDbHandle();

    /**
     * @return type factory to be used for any type-checking during
     * validation
     */
    public FarragoTypeFactory getTypeFactory();

    /**
     * @return the FarragoSessionIndexMap to use for managing index storage
     */
    public FarragoSessionIndexMap getIndexMap();

    /**
     * @return cache for loaded data wrappers
     */
    public FarragoDataWrapperCache getDataWrapperCache();

    /**
     * @return the FarragoSession which invoked this DDL
     */
    public FarragoSession getInvokingSession();

    /**
     * @return the parser whose statement is being validated
     */
    public FarragoSessionParser getParser();

    /**
     * @return is a DROP RESTRICT being executed?
     */
    public boolean isDropRestrict();

    /**
     * Creates a new reentrant session; this is required during validation of
     * some DDL commands (e.g. view creation) for preparation or execution of
     * internal SQL statements.
     *
     * @return a FarragoSession to use for reentrant SQL; when done, this
     * session must be released by calling releaseReentrantSession
     */
    public FarragoSession newReentrantSession();

    /**
     * Releases a FarragoSession acquired with newReentrantSession().
     *
     * @param session the session to release
     */
    public void releaseReentrantSession(FarragoSession session);

    /**
     * Determines whether a catalog object is being deleted by this DDL
     * statement.
     *
     * @param refObject object in question
     *
     * @return true if refObject is being deleted
     */
    public boolean isDeletedObject(RefObject refObject);

    /**
     * Determines whether a catalog object is being created by this DDL
     * statement.
     *
     * @param refObject object in question
     *
     * @return true if refObject is being created
     */
    public boolean isCreatedObject(RefObject refObject);

    /**
     * Finds the parse position for an object affected by DDL.  Not all objects
     * have parse positions (e.g. when a table is dropped, referencing views
     * are implicitly affected).
     *
     * @param obj the affected object
     *
     * @return the parse position string associated with the given object, or
     * null if the object was not encountered by parsing
     */
    public String getParserPosString(RefObject obj);

    /**
     * Sets the parser offset for the body of a given object.
     *
     * @param obj object being defined
     *
     * @param pos parser offset
     */
    public void setParserOffset(RefObject obj, SqlParserPos pos);

    /**
     * Retrieves the parser offset for the body of a given object.
     *
     * @param obj object to look up
     *
     * @return parser offset, or null if none recorded
     */
    public SqlParserPos getParserOffset(RefObject obj);

    /**
     * Associates an SQL definition with a catalog object.  This is called
     * from the parser; later, this information is retrieved during validation
     * via {@link #getSqlDefinition}.
     *
     * @param obj object being defined
     *
     * @param sqlNode SQL definition
     */
    public void setSqlDefinition(RefObject obj, SqlNode sqlNode);

    /**
     * Retrieves an SQL definition previously associated with a catalog
     * object via {@link #setSqlDefinition}.  As a side effect, also
     * restores parser context for this object if available.
     *
     * @param obj object being validated
     *
     * @return SQL definition
     */
    public SqlNode getSqlDefinition(RefObject obj);

    /**
     * Sets the name of a new object being defined, and adds the object to
     * the correct schema.
     *
     * @param schemaElement the object being named
     *
     * @param qualifiedName the (possibly) qualified name of the object
     */
    public void setSchemaObjectName(
        CwmModelElement schemaElement,
        SqlIdentifier qualifiedName);

    /**
     * Executes storage management commands for any model elements
     * encountered during validation.
     */
    public void executeStorage();

    /**
     * Schedules truncation of an existing object.
     *
     * @param modelElement to be truncated
     */
    public void scheduleTruncation(CwmModelElement modelElement);

    /**
     * Validates all scheduled operations.  Validation may cause other objects
     * to be changed, so the process continues until a fixpoint is reached.
     *
     * @param ddlStmt DDL statement to be validated
     */
    public void validate(FarragoSessionDdlStmt ddlStmt);

    /**
     * Determines whether all of the objects in a collection have distinct
     * names, throwing an appropriate exception if not.
     *
     * @param container namespace object for use in error message
     *
     * @param collection Collection of CwmModelElements representing namespace
     *        contents
     *
     * @param includeType if true, include type in name; if false, ignore
     *        types in deciding uniqueness
     */
    public void validateUniqueNames(
        CwmModelElement container,
        Collection collection,
        boolean includeType);

    /**
     * Creates a new dependency.
     *
     * @param client element which depends on others
     * (FIXME:  this should be a CwmModelElement, not a CwmNamespace)
     *
     * @param suppliers collection of elements on which client depends
     *
     * @param kind name for dependency type
     */
    public CwmDependency createDependency(
        CwmNamespace client,
        Collection suppliers,
        String kind);

    /**
     * Discards a data wrapper or server from the shared cache
     * (called when it is dropped).
     *
     * @param wrapper definition of wrapper to discard
     */
    public void discardDataWrapper(CwmModelElement wrapper);

    /**
     * Defines the handlers to be used to validate and execute DDL actions
     * for various object types.  See {@link FarragoSessionDdlHandler}
     * for an explanation of how to define the handler objects in this
     * list.
     *
     * @return list of handler objects
     */
    public List defineHandlers();

    /**
     * Sets the context for a compound CREATE SCHEMA statement to be used
     * by all object definitions in the new schema.
     *
     * @param schema new schema being created
     */
    public void setCreatedSchemaContext(FemLocalSchema schema);

    /**
     * Wraps a validation error with position information.
     *
     * @param refObj object whose definition should be used for
     * position information
     *
     * @param ex exception to be wrapped
     *
     * @return wrapping exception
     */
    public EigenbaseException newPositionalError(
        RefObject refObj,
        SqlValidatorException ex);
}


// End FarragoSessionDdlValidator.java
