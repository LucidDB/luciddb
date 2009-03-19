/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.sql.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;


/**
 * FarragoSessionVariables defines global variable settings for a Farrago
 * session. It has two types of variables, plain old Java object fields, and
 * variables stored in a generic name to value map. Plain old Java object fields
 * are accessed directly, while mapped fields are accessed with {@link
 * #set(String, String)} and {@link #get(String)}. Validation is handled in
 * {@link FarragoSessionPersonality}.
 *
 * <pre><code>
 * Example:
 * FarragoSessionVariables sessionVars = ...;
 * String catalogName = sessionVars.catalogName;
 * sessionVars.set("newVar", "value"); // registers and sets value in map
 * String newVar = sessionVars.get("newVar"); // gets mapped value only
 * </code></pre>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionVariables
    implements Cloneable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of session variable defining directory for log files; this is not
     * currently used by the default Farrago personality, but it serves as a
     * canonical name across all other personalities which need a similar
     * concept. It may be used by the default Farrago personality in the future.
     */
    public static final String LOG_DIR = "logDir";

    //~ Instance fields --------------------------------------------------------

    /**
     * The name of the default catalog qualifier, changed by SET CATALOG. Can
     * never be null.
     */
    public String catalogName;

    /**
     * The name of the default schema qualifier, changed by SET SCHEMA. Can be
     * null to indicate no default schema has been set yet.
     */
    public String schemaName;

    /**
     * Value of SQL expression SYSTEM_USER.
     */
    public String systemUserName;

    /**
     * Value of SQL expression SESSION_USER.
     */
    public String sessionUserName;

    /**
     * Value of SQL expression CURRENT_USER.
     */
    public String currentUserName;

    /**
     * Value of SQL expression CURRENT_ROLE.
     */
    public String currentRoleName;

    /**
     * Value of SQL expression CURRENT_PATH as a list of schemas. Each entry is
     * a {@link SqlIdentifier} (catalog.schema). This list is immutable to
     * prevent accidental aliasing.
     */
    public List<SqlIdentifier> schemaSearchPath;

    /**
     * Full user name, e.g. "Joe Smith". Can be null. TODO: Value of SQL
     * expression SYSTEM_USER_FULLNAME?.
     */
    public String systemUserFullName;

    /**
     * Session name. Can be null if session has no name. TODO: Value of SQL
     * expression SESSION_NAME?
     */
    public String sessionName;

    /**
     * Client program name. Can be null. TODO: Value of SQL expression
     * PROGRAM_NAME?
     */
    public String programName;

    /**
     * Client process Id. TODO: Value of SQL expression PROCESS_ID?
     */
    public long processId;

    /**
     * Additional variables
     */
    private Map<String, String> valueMap;

    //~ Constructors -----------------------------------------------------------

    public FarragoSessionVariables()
    {
        valueMap = new HashMap<String, String>();
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoSessionVariables cloneVariables()
    {
        try {
            FarragoSessionVariables copy = (FarragoSessionVariables) clone();

            // Perform a deep copy of the value map
            copy.valueMap = new HashMap<String, String>();
            copy.valueMap.putAll(valueMap);
            return copy;
        } catch (CloneNotSupportedException ex) {
            throw new AssertionError();
        }
    }

    /**
     * Format the schema search path as required by the SQL99 standard.
     *
     * @param databaseMetadata current database metadata
     *
     * @return formated schema search path, as per the SQL standard
     *
     * @sql.99 Part 2 Section 6.3 General Rule 10
     */
    public String getFormattedSchemaSearchPath(
        DatabaseMetaData databaseMetadata)
    {
        // The SQL standard is very precise about the formatting
        SqlDialect dialect = new SqlDialect(databaseMetadata);
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        StringBuilder buf = new StringBuilder();
        int k = 0;
        for (SqlIdentifier id : schemaSearchPath) {
            if (k++ > 0) {
                buf.append(",");
            }
            id.unparse(writer, 0, 0);
            buf.append(writer.toString());
            writer.reset();
        }
        return buf.toString();
    }

    // REVIEW jvs 9-Jan-2007:  copyVariables is not used anywhere.
    // Does it have a purpose?

    /**
     * Copy the values in <code>baseVariables</code> to this instance. Allows
     * extensions projects to provide extend FarragoSessionVariables and those
     * new session variables to values from an existing FarragoSessionVariables.
     *
     * @param baseVariables an existing FarragoSessionVariables to copy into
     * <code>this</code>.
     */
    protected void copyVariables(FarragoSessionVariables baseVariables)
    {
        this.catalogName = baseVariables.catalogName;
        this.schemaName = baseVariables.schemaName;
        this.systemUserName = baseVariables.systemUserName;
        this.sessionUserName = baseVariables.sessionUserName;
        this.currentUserName = baseVariables.currentUserName;
        this.processId = baseVariables.processId;
        this.sessionName = baseVariables.sessionName;

        // Since schemaSearchPath is meant to be an unmodifiable list,
        // it should be okay to just copy the reference.
        this.schemaSearchPath = baseVariables.schemaSearchPath;

        // Perform a deep copy of the value map
        this.valueMap = new HashMap<String, String>();
        this.valueMap.putAll(baseVariables.valueMap);
    }

    /**
     * Sets the value of a variable in a generic value map. This method does not
     * affect any variables explictly materialized as a public members.
     *
     * @param name the name of a session variable
     * @param value the value to set, expressed as a string
     */
    public void set(String name, String value)
    {
        valueMap.put(name, value);
    }

    /**
     * Gets the value of variable in a generic value map.
     *
     * @param name the name of a session variable
     *
     * @throws IllegalArgumentException if the variable is not in the map
     */
    public String get(String name)
    {
        if (!valueMap.containsKey(name)) {
            throw new IllegalArgumentException();
        }
        return valueMap.get(name);
    }

    /**
     * Tests whether a session variable is defined in this map.
     *
     * @param name variable name to check
     *
     * @return true if variable is defined
     */
    public boolean containsVariable(String name)
    {
        return valueMap.containsKey(name);
    }

    /**
     * Sets the value of a variable, expressed as an integer
     *
     * @see #set(String, String)
     */
    public void setInteger(String name, Integer value)
    {
        String stringValue = (value == null) ? null : value.toString();
        valueMap.put(name, stringValue);
    }

    /**
     * Gets the value of a variable, casted to an Integer
     *
     * @param name the name of a session variable
     *
     * @throws IllegalArgumentException if the variable is not in the map
     * @throws NumberFormatException if the value cannot be casted to an Integer
     *
     * @see #get(String)
     */
    public Integer getInteger(String name)
    {
        String stringValue = get(name);
        return (stringValue == null) ? null : Integer.valueOf(stringValue);
    }

    /**
     * Sets the value of a variable, expressed as a long
     *
     * @see #set(String, String)
     */
    public void setLong(String name, Long value)
    {
        String stringValue = (value == null) ? null : value.toString();
        valueMap.put(name, stringValue);
    }

    /**
     * Gets the value of a variable, casted to a Long
     *
     * @param name the name of a session variable
     *
     * @throws IllegalArgumentException if the variable is not in the map
     * @throws NumberFormatException if the value cannot be casted to a Long
     *
     * @see #get(String)
     */
    public Long getLong(String name)
    {
        String stringValue = get(name);
        return (stringValue == null) ? null : Long.valueOf(stringValue);
    }

    /**
     * Sets the value of a variable, expressed as an boolean
     *
     * @see #set(String, String)
     */
    public void setBoolean(String name, Boolean value)
    {
        String stringValue = (value == null) ? null : value.toString();
        valueMap.put(name, stringValue);
    }

    /**
     * Gets the value of a variable, casted to a Boolean
     *
     * @param name the name of a session variable
     *
     * @throws IllegalArgumentException if the variable is not in the map
     *
     * @see #get(String)
     */
    public Boolean getBoolean(String name)
    {
        String stringValue = get(name);
        return (stringValue == null) ? null : Boolean.valueOf(stringValue);
    }

    /**
     * Sets the default value for a variable. Does nothing if the variable has
     * already been initialized.
     *
     * @param name the name of the variable
     * @param value the default value of a variable
     */
    public void setDefault(String name, String value)
    {
        if (!valueMap.containsKey(name)) {
            valueMap.put(name, value);
        }
    }

    /**
     * Retrieves a read only map from parameter name to parameter value.
     * Parameter values are expressed as strings. This map contains values of
     * the generic value map. It also contains public fields of {@link
     * #FarragoSessionVariables}. The public fields take precedence.
     */
    public Map<String, String> getMap()
    {
        Map<String, String> readMap = new HashMap<String, String>();
        readMap.putAll(valueMap);

        // copy public fields
        readMap.put("catalogName", catalogName);
        readMap.put("schemaName", schemaName);
        readMap.put("systemUserName", systemUserName);
        readMap.put("sessionUserName", sessionUserName);
        readMap.put("currentUserName", currentUserName);
        readMap.put("currentRoleName", currentRoleName);

        // TODO: schemaSearchPath
        readMap.put("systemUserFullName", systemUserFullName);
        readMap.put("sessionName", sessionName);
        readMap.put("programName", programName);
        readMap.put("processId", Long.toString(processId));

        return Collections.unmodifiableMap(readMap);
    }
}

// End FarragoSessionVariables.java
