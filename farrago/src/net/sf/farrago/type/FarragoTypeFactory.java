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
package net.sf.farrago.type;

import java.sql.*;

import javax.jmi.model.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;

import org.eigenbase.oj.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;


/**
 * FarragoTypeFactory is a Farrago-specific refinement of the
 * RelDataTypeFactory interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoTypeFactory extends OJTypeFactory
{
    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return associated FarragoRepos
     */
    public FarragoRepos getRepos();

    /**
     * Creates a FarragoType which represents the datatype of a CWM column.
     *
     * @param column CWM column
     *
     * @param validated if true, the column's definition has already been
     * validated, and the returned type will be complete; if false, the
     * returned type will have only enough information needed for
     * column DDL validation
     *
     * @return generated FarragoType
     */
    public FarragoType createColumnType(
        CwmColumn column,
        boolean validated);

    /**
     * Creates a type which represents the row datatype of a CWM
     * ColumnSet
     *
     * @param columnSet CWM ColumnSet
     *
     * @return generated type, or null if columnSet had no columns defined yet
     */
    public RelDataType createColumnSetType(CwmColumnSet columnSet);

    /**
     * Creates a type which represents the row datatype of a JDBC
     * ResultSet.
     *
     * @param metaData metadata for JDBC ResultSet
     *
     * @return generated type
     */
    public RelDataType createResultSetType(ResultSetMetaData metaData);

    /**
     * Creates a FarragoType which represents a MOF feature.
     *
     * @param feature MOF feature
     *
     * @return generated FarragoType
     */
    public FarragoType createMofType(StructuralFeature feature);

    /**
     * Initializes a CwmColumn definition based on a RelDataTypeField.
     * If the column has no name, the name is initialized from the field
     * name; otherwise, the existing name is left unmodified.
     *
     * @param field input field (must have a FarragoType)
     *
     * @param column on input, contains unintialized CwmColumn instance;
     * on return, this has been initialized (but not validated)
     */
    public void convertFieldToCwmColumn(
        RelDataTypeField field,
        CwmColumn column);
}


// End FarragoTypeFactory.java
