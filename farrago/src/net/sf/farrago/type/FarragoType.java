/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import openjava.mop.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;


/**
 * FarragoType is the abstract superclass for all Farrago-specific
 * implementations of the RelDataType interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoType implements RelDataType
{
    //~ Instance fields -------------------------------------------------------

    /** Computed digest. */
    protected String digest;

    /** Owning FarragoTypeFactory. */
    FarragoTypeFactory factory;

    //~ Methods ---------------------------------------------------------------

    // implement RelDataType
    public RelDataTypeFactory getFactory()
    {
        return factory;
    }

    // implement RelDataType
    public boolean isStruct()
    {
        return false;
    }

    // override Object
    public boolean equals(Object obj)
    {
        if (obj instanceof FarragoType) {
            final FarragoType that = (FarragoType) obj;
            return this.digest.equals(that.digest);
        }
        return false;
    }

    // override Object
    public int hashCode()
    {
        return digest.hashCode();
    }

    /**
     * Get the OJClass representing this FarragoType.
     *
     * @param declarer the outer OJClass which should be used if a new inner
     *        class needs to be generated
     *
     * @return the representative OJClass
     */
    protected abstract OJClass getOjClass(OJClass declarer);

    /**
     * Compute a digest for this type and store it in the digest field.
     */
    protected abstract void computeDigest();

    protected FarragoTypeFactoryImpl getFactoryImpl()
    {
        return (FarragoTypeFactoryImpl) factory;
    }

    // implement RelDataType
    public boolean isAssignableFrom(
        RelDataType t,
        boolean coerce)
    {
        return this.getFactory().assignableFrom(
            this.getSqlTypeName(),
            t.getSqlTypeName(),
            coerce);
    }
}


// End FarragoType.java
