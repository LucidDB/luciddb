/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.reltype;

import java.nio.charset.Charset;

import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.SqlTypeName;


/**
 * The type of a scalar expression or a row returned from a relational
 * expression.
 *
 *<p>
 *
 * This is a "fat" interface which unions the attributes of many different type
 * classes into one.  Inelegant, but since it was defined before the advent of
 * Java generics, it avoided a lot of typecasting.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 29, 2003
 */
public interface RelDataType
{
    //~ Methods ---------------------------------------------------------------

    RelDataTypeField getField(String fieldName);

    /**
     * @return true if this type has fields; examples include rows and
     * user-defined structured types in SQL, and classes in Java
     */
    boolean isStruct();

    /**
     * @return the number of fields in a struct type; result is
     * undefined for a non-struct type
     */
    int getFieldCount();

    int getFieldOrdinal(String fieldName);

    /**
     * @return the fields in a struct type; result is undefined
     * for a non-struct type
     *
     * @post return != null
     */
    RelDataTypeField [] getFields();

    /**
     * @return whether this type allows null values.
     */
    boolean isNullable();

    /**
     * Returns the component type if type is a collection, otherwise null.
     */
    RelDataType getComponentType();

    // REVIEW jvs 1-Mar-2004:  The implementations for this method
    // are asymmetric, e.g. INT.isAssignableFrom(SMALLINT) but
    // not vice-versa (due to possible overflow?).  The SQL definition of
    // assignability is symmetric (e.g. overflow is a runtime check).  So
    // maybe the name of this method should be something like
    // isSupertypeOf instead?

    /**
     * Returns whether a value of this type can be assigned from a value of
     * a given other type.
     */
    boolean isAssignableFrom(
        RelDataType t,
        boolean coerce);

    /**
     * Returns this type's character set, or null if this type can carry a
     * character set but no character set is defined.
     *
     * @throws RuntimeException if this type is not of a kind (char,
     *   varchar, and so forth) that can carry a character set.
     */
    Charset getCharset()
        throws RuntimeException;

    /**
     * Returns this type's collation, or null if this type can carry a
     * collation but no collation is defined.
     *
     * @throws RuntimeException if this type is not of a kind (char,
     *   varchar, and so forth) that can carry a collation.
     */
    SqlCollation getCollation()
        throws RuntimeException;

    /**
     * @return number of digits or characters of precision
     */
    public int getPrecision();

    /**
     * @return number of digits of scale
     */
    public int getScale();

    /**
     * @return the SqlTypeName for this RelDataType, or null if
     * it is not an SQL type
     */
    public SqlTypeName getSqlTypeName();

    /**
     * @return this type as a string without detail
     * such as character set and nullability
     */
    public String toString();

    /**
     * Compute a string from this type with full detail such as character set
     * and nullability.  This string must serve as a "digest" for the type,
     * meaning two types can be considered identical iff their digests are
     * equal.
     *
     * @return the full type string
     */
    public String getFullTypeString();

    /**
     * @return a canonical object representing the family of this type
     */
    public RelDataTypeFamily getFamily();
}


// End RelDataType.java
