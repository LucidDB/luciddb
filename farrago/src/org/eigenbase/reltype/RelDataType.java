/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package org.eigenbase.reltype;

import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.SqlTypeName;

import java.nio.charset.Charset;

/**
 * The type of a scalar expression or a row returned from a relational
 * expression.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 29, 2003
 */
public interface RelDataType
{
    //~ Methods ---------------------------------------------------------------

    RelDataTypeFactory getFactory();

    RelDataTypeField getField(String fieldName);

    /**
     * Returns the number of columns.
     */
    int getFieldCount();

    int getFieldOrdinal(String fieldName);

    /**
     * Returns the columns.
     *
     * @post return != null
     */
    RelDataTypeField [] getFields();

    /**
     * Whether the type represents a cartesian product of regular types.
     */
    boolean isJoin();

    /**
     * Returns the component types of a join type.
     *
     * @pre isJoin()
     */
    RelDataType [] getJoinTypes();

    boolean isProject();

    /**
     * Whether this type is identical to another, save for differences in
     * nullability.
     */
    boolean equalsSansNullability(RelDataType type);

    /**
     * @return whether this type allows null values.
     */
    boolean isNullable();

    /**
     * Returns the component type if type is a collection, otherwise null.
     */
    RelDataType getComponentType();

    /**
     * Returns an array type with this type as the component.
     */
    RelDataType getArrayType();

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
    boolean isAssignableFrom(RelDataType t, boolean coerce);

    /**
     * Returns whether two values are of the same type. E.g.
     *  varchar(5) and varchar(0), are of the same type
     *  double and float, aren't
     */
    boolean isSameType(RelDataType t);

    /**
     * Returns whether two values are of the same type family. E.g.
     *  varchar(5) and varchar(0), are of the same type family
     *  double, float, int, bigint, are of the same type family
     *  varchar(x) and int are NOT of the same type family
     */
    boolean isSameTypeFamily(RelDataType t);

    /**
     * If type represent a char, varchar or any other type that can carry a collation
     * this function must return true, otherwise returns false. <BR>
     */
    boolean isCharType();

    /**
     * Returns this type's character set, or null if this type can carry a
     * character set but no character set is defined.
     *
     * @throws RuntimeException if this type is not of a kind (char,
     *   varchar, and so forth) that can carry a character set.
     */
    Charset getCharset();

    /**
     * Returns this type's collation, or null if this type can carry a
     * collation but no collation is defined.
     *
     * @throws RuntimeException if this type is not of a kind (char,
     *   varchar, and so forth) that can carry a collation.
     */
    SqlCollation getCollation() throws RuntimeException;

    /**
     * Returns the maximum number of bytes storage required to store a value
     * of this type. If the type is fixed-length, returns -1.
     */
    int getMaxBytesStorage();

    /**
     * @return number of digits or characters of precision
     */
    public int getPrecision();

    /**
     * get the SqlTypeName for this RelDataType.
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
}


// End RelDataType.java
