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

package net.sf.farrago.runtime;

/**
 * NullableValue is an interface representing a runtime holder for a nullable
 * object.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface NullableValue
{
    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_ACCESSOR_NAME = "isNull";
    
    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_MUTATOR_NAME = "setNull";
    
    //~ Methods ---------------------------------------------------------------

    /**
     * Set whether or not the value is null.  Note that once a value has been
     * set to null, its data should not be updated until the null state has
     * been cleared with a call to setNull(false).
     *
     * @param isNull true to set a null value; false to indicate a non-null
     *        value
     */
    void setNull(boolean isNull);

    /**
     * .
     *
     * @return whether the value has been set to null
     */
    boolean isNull();

    /**
     * .
     *
     * @return an Object representation of this value's data, or null if this
     *         value is null
     */
    Object getNullableData();
}


// End NullableValue.java
