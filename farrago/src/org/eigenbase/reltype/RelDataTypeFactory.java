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

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.SqlCollation;

import java.nio.charset.Charset;


/**
 * Creates types.
 * 
 * <p>
 * Any implementation of <code>RelDataTypeFactory</code> must ensure that
 * types are canonical: two types are equal if and only if they are the same
 * object.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 29, 2003
 */
public interface RelDataTypeFactory
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a type which encapsulates a Java class.
     */
    RelDataType createJavaType(Class clazz);

    /**
     * Creates a cartesian product type.
     *
     * @pre types != null
     * @pre types.length >= 1
     */
    RelDataType createJoinType(RelDataType [] types);

    /**
     * Creates a type which represents a projection of a set of fields.
     * 
     * <p>
     * The return is canonical: if an equivalent type already exists, it is
     * returned.
     * </p>
     *
     * @param types Types of the fields
     * @param fieldNames Names of the fields
     *
     * @return a type
     *
     * @pre types.length == fieldNames.length
     * @post return != null
     */
    RelDataType createProjectType(RelDataType [] types,String [] fieldNames);

    /**
     * Creates a type which represents a projection of a set fields, getting
     * the field informatation from a callback.
     */
    RelDataType createProjectType(FieldInfo fieldInfo);

    /**
     * Duplicates a type; makes a deep copy of a record type.
     * @param type input type
     * @return output type, a new object equivalent to input type.
     */
    RelDataType copyType(RelDataType type);

    /**
     * Creates a Type which is the same as another type but with possibily
     * different nullability. The output type may be identical to the input type.
     * For type systems without a concept of
     * nullability, the return value is always the same as the input.
     *
     * @param type input type
     *
     * @param nullable true to request a nullable type; false to request a
     * NOT NULL type
     *
     * @return output type, same as input type except with specified nullability
     */
    RelDataType createTypeWithNullability(RelDataType type,boolean nullable);

    /**
     * Creates a Type which is the same as another type but with possibily
     * different charset or collation. For types without a concept of
     * charset or collation this function must throw an error
     * @pre type.isCharType()==true
     * @param type input type
     * @return output type, same as input type except with specified charset
     * and collation
     */
    RelDataType createTypeWithCharsetAndCollation(RelDataType type,
            Charset charset, SqlCollation collation);

    /**
     * Returns the most general of a set of types (that is, one type to which
     * they can all be cast), or null if conversion is not possible.
     *
     * @pre types != null
     * @pre types.length >= 1
     */
    RelDataType leastRestrictive(RelDataType [] types);


    // NOTE jvs 18-Dec-2003: I changed the createSqlType methods to return a
    // RelDataType instead of a RelDataTypeFactoryImpl.SqlType.  I know that
    // SqlType is going to move out sometime soon, but I needed to be able to
    // override these methods to return FarragoAtomicTypes instead.  Once a
    // proper SqlType interface is defined, FarragoType should be changed to
    // implement it, and then these methods can return SqlType again.
    
    /**
     * Creates a SQL type with no precision or scale.
     *
     * @param typeName Name of the type, for example
     *   {@link SqlTypeName#Boolean}.
     * @return A type
     * @pre typeName != null
     * @post return != null
     */
    RelDataType createSqlType(SqlTypeName typeName);

    /**
     * Creates a SQL type with length (precision) but no scale.
     *
     * @param typeName Name of the type, for example
     *   {@link org.eigenbase.sql.type.SqlTypeName#Varchar}.
     * @param length Maximum length of the value (non-numeric types)
     *   or the precision of the value (numeric types)
     * @return A type
     * @pre typeName != null
     * @pre length >= 0
     * @post return != null
     */
    RelDataType createSqlType(SqlTypeName typeName, int length);

    /**
     * Creates a SQL type with length (precision) and scale.
     * 
     * @param typeName Name of the type, for example
     *   {@link org.eigenbase.sql.type.SqlTypeName#Varchar}.
     * @param length Maximum length of the value (non-numeric types)
     *   or the precision of the value (numeric types)
     * @param scale Scale of the values. The number of decimal places to shift
     *   the value. For example, a NUMBER(10,3) value of "123.45" is
     *   represented "123450" (that is, multiplied by 10^3). A negative scale
     *   <em>is</em> valid.
     * @return A type
     * @pre typeName != null
     * @pre length >= 0
     * @post return != null
     */
    RelDataType createSqlType(SqlTypeName typeName, int length, int scale);

    //~ Inner Interfaces ------------------------------------------------------

    /**
     * Callback which provides enough information to create fields.
     */
    interface FieldInfo
    {
        int getFieldCount();

        String getFieldName(int index);

        RelDataType getFieldType(int index);
    }
    /**
     * coerce=true implies that explicit rather than implicit cast is being
     * done.
     **/
    public boolean assignableFrom(SqlTypeName to, SqlTypeName from,
            boolean coerce);
}


// End RelDataTypeFactory.java
