/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.core;

import net.sf.saffron.sql.SqlCollation;

import java.io.PrintWriter;
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
public interface SaffronType
{
    //~ Methods ---------------------------------------------------------------

    SaffronTypeFactory getFactory();

    SaffronField getField(String fieldName);

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
    SaffronField [] getFields();

    /**
     * Whether the type represents a cartesian product of regular types.
     */
    boolean isJoin();

    /**
     * Returns the component types of a join type.
     *
     * @pre isJoin()
     */
    SaffronType [] getJoinTypes();

    boolean isProject();

    /**
     * Whether this type is identical to another, save for differences in
     * nullability.
     */
    boolean equalsSansNullability(SaffronType type);

    /**
     * @return whether this type allows null values.
     */
    boolean isNullable();

    /**
     * Prints a value of this type.
     */
    void format(Object value, PrintWriter pw);

    /**
     * Returns the component type if type is a collection, otherwise null.
     */
    SaffronType getComponentType();

    /**
     * Returns an array type with this type as the component.
     */ 
    SaffronType getArrayType();

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
    boolean isAssignableFrom(SaffronType t);

    /**
     * Returns whether two values are of the same type family. E.g.
     *  varchar(5) and varchar(0), belong to the same type family
     *  double and float, don't
     */
    boolean isSameTypeFamily(SaffronType t);

    /**
     * If type represent a string, char, varchar or any other type that can carry a collation
     * this function must return true, otherwise returns false. <BR>
     * Comments: Technically this function is not needed. Its possible to do the same with multiple calls to
     * {@link #isSameTypeFamily}, however having this function is convenient and doesn't only simplify code where
     * {@link #getCharset}, {@link #setCharset}, {@link #getCollation} and {@link #setCollation}
     * are used but also slightly simplifies the implementations of the named functions.
     */
    boolean isCharType();

    /**
     * If type represent a string, char, varchar or any other type that can carry a character set
     * this function will return the charset or null if not defined.
     * If type can not carry a charset an runtime exception MUST be thrown.
     */
    Charset getCharset();

    /**
     * If type represent a string, char, varchar or any other type that can carry a character set
     * this function will set the charset
     * If type can not carry a charset an runtime exception MUST be thrown.
     */
     void setCharset(Charset charset);

    /**
     * If type represent a string, char, varchar or any other type that can carry a collation this function will return
     * the collation or null if not defined.
     * If type can not carry a collation an runtime exception MUST be thrown.
     */
    SqlCollation getCollation() throws RuntimeException;

    /**
     * If type represent a string, char, varchar or any other type that can carry a collation this function will set
     * the collation.
     * If type can not carry a collation an runtime exception MUST be thrown.
     */
    void setCollation(SqlCollation collation) throws RuntimeException;
}


// End SaffronType.java
