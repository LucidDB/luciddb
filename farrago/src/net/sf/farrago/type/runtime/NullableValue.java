/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.type.runtime;

/**
 * NullableValue is an interface representing a runtime holder for a nullable
 * object.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface NullableValue
    extends DataValue
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_ACCESSOR_NAME = "isNull";

    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_MUTATOR_NAME = "setNull";

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets whether or not the value is null. Note that once a value has been
     * set to null, its data should not be updated until the null state has been
     * cleared with a call to setNull(false).
     *
     * @param isNull true to set a null value; false to indicate a non-null
     * value
     */
    void setNull(boolean isNull);

    /**
     * @return whether the value has been set to null
     */
    boolean isNull();
}

// End NullableValue.java
