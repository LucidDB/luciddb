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
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.oj.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;

import openjava.ptree.*;

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
     * Creates a type which represents the datatype of a FemSqltypedElement.
     *
     * @param element CWM typed element
     *
     * @return generated type
     */
    public RelDataType createCwmElementType(
        FemSqltypedElement element);

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
     * Creates a type which represents a MOF feature.
     *
     * @param feature MOF feature
     *
     * @return generated type
     */
    public RelDataType createMofType(StructuralFeature feature);

    /**
     * Constructs an OpenJava expression to access a value of an
     * atomic type.
     *
     * @param type atomic type
     *
     * @param expr expression representing site to be accessed
     *
     * @return expression for accessing value
     */
    public Expression getValueAccessExpression(
        RelDataType type,
        Expression expr);

    /**
     * Looks up the java.lang.Class representing a primitive used to
     * hold a value of the given type.
     *
     * @param type value type
     *
     * @return primitive Class, or null if a non-primitive Object is used
     * at runtime to represent values of the given type
     */
    public Class getClassForPrimitive(
        RelDataType type);
}


// End FarragoTypeFactory.java
