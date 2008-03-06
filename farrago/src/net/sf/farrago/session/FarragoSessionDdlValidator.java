/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
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
package net.sf.farrago.session;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * FarragoSessionDdlValidator represents an object capable of validating a DDL
 * statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionDdlValidator
    extends FarragoAllocation
{
    //~ Methods ----------------------------------------------------------------

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
     * @return type factory to be used for any type-checking during validation
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
     * Finds the parse position for an object affected by DDL. Not all objects
     * have parse positions (e.g. when a table is dropped, referencing views are
     * implicitly affected).
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
     * <p>If the body text has preceding whitespace, trims the whitespace and
     * advances the position by the number of whitespace characters removed.
     * Returns the trimmed body.
     *
     * @param obj object being defined
     * @param pos parser offset
     * @param body text of body
     * @return body text with preceding whitespace removed
     */
    public String setParserOffset(
        RefObject obj,
        SqlParserPos pos,
        String body);

    /**
     * Retrieves the parser offset for the body of a given object.
     *
     * @param obj object to look up
     *
     * @return parser offset, or null if none recorded
     */
    public SqlParserPos getParserOffset(RefObject obj);

    /**
     * Associates an SQL definition with a catalog object. This is called from
     * the parser; later, this information is retrieved during validation via
     * {@link #getSqlDefinition}.
     *
     * @param obj object being defined
     * @param sqlNode SQL definition
     */
    public void setSqlDefinition(RefObject obj, SqlNode sqlNode);

    /**
     * Retrieves an SQL definition previously associated with a catalog object
     * via {@link #setSqlDefinition}. As a side effect, also restores parser
     * context for this object if available.
     *
     * @param obj object being validated
     *
     * @return SQL definition
     */
    public SqlNode getSqlDefinition(RefObject obj);

    /**
     * Sets the name of a new object being defined, and adds the object to the
     * correct schema.
     *
     * @param schemaElement the object being named
     * @param qualifiedName the (possibly) qualified name of the object
     */
    public void setSchemaObjectName(
        CwmModelElement schemaElement,
        SqlIdentifier qualifiedName);

    /**
     * Executes storage management commands for any model elements encountered
     * during validation.
     */
    public void executeStorage();

    /**
     * Validates all scheduled operations. Validation may cause other objects to
     * be changed, so the process continues until a fixpoint is reached.
     *
     * @param ddlStmt DDL statement to be validated
     */
    public void validate(FarragoSessionDdlStmt ddlStmt);

    /**
     * Determines whether all of the objects in a collection have distinct
     * names, throwing an appropriate exception if not.
     *
     * @param container namespace object for use in error message
     * @param collection Collection of CwmModelElements representing namespace
     * contents
     * @param includeType if true, include type in name; if false, ignore
     */
    public void validateUniqueNames(
        CwmModelElement container,
        Collection<? extends CwmModelElement> collection,
        boolean includeType);

    /**
     * validate the names provided in a VIEW's explicit column list.
     *
     * @param collection Collection of CwmModelElements representing the
     * explicitly named columns
     */
    public void validateViewColumnList(Collection<?> collection);

    /**
     * Creates a new dependency.
     *
     * @param client element which depends on others; we require this to be a
     * {@link CwmNamespace} so that it can own the {@link CwmDependency} created
     * @param suppliers collection of elements on which client depends
     *
     * @return new dependency
     */
    public <T extends CwmModelElement> CwmDependency createDependency(
        CwmNamespace client,
        Collection<T> suppliers);

    /**
     * Discards a data wrapper or server from the shared cache (called when it
     * is dropped).
     *
     * @param wrapper definition of wrapper to discard
     */
    public void discardDataWrapper(CwmModelElement wrapper);

    /**
     * Sets the context for a compound CREATE SCHEMA statement to be used by all
     * object definitions in the new schema.
     *
     * @param schema new schema being created
     */
    public void setCreatedSchemaContext(FemLocalSchema schema);

    /**
     * Wraps a validation error with position information.
     *
     * @param refObj object whose definition should be used for position
     * information
     * @param ex exception to be wrapped
     *
     * @return wrapping exception
     */
    public EigenbaseException newPositionalError(
        RefObject refObj,
        SqlValidatorException ex);

    /**
     * Adds a {@link FarragoSessionDdlDropRule}.
     *
     * @param refAssoc model association to which the rule relates
     * @param dropRule rule to add
     */
    public void defineDropRule(
        RefAssociation refAssoc,
        FarragoSessionDdlDropRule dropRule);

    /**
     * Called after revalidation (validation of dependencies during a CREATE OR
     * REPLACE) is successful (ex is null), or upon failure (ex is not null).
     *
     * @param element object impacted by replacement
     * @param ex exception to be handled, may be null
     */
    public void setRevalidationResult(
        CwmModelElement element,
        EigenbaseException ex);

    /**
     * Returns immediate dependencies of an of element.
     *
     * @param rootElement Starting element for dependency search
     *
     * @return Set of CwmModelElement, immediate dependencies of rootElement
     */
    public Set<CwmModelElement> getDependencies(CwmModelElement rootElement);

    /**
     * Modifies the analyzed SQL for a view definition, to take into account any
     * system columns which a personality may need to have in the view. In
     * particular, makes sure that the analyzed SQL returns the same number and
     * type of columns as the view definition.
     *
     * @param view View definition
     * @param analyzedSql Analyzed SQL for the view definition
     */
    void fixupView(FemLocalView view, FarragoSessionAnalyzedSql analyzedSql);
    
    /**
     * Obtains the single consistent timestamp for this DDL transaction.
     * 
     * @return the timestamp for this DDL transaction
     */
    public String obtainTimestamp();
}

// End FarragoSessionDdlValidator.java
