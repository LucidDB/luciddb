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

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.runtime.*;

import net.sf.saffron.util.*;

import openjava.mop.*;


/**
 * FarragoPrimitiveType instances represent CwmSqlsimpleTypes which can be
 * implemented as Java primitives (or their corresponding NullableValue
 * wrappers).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public final class FarragoPrimitiveType extends FarragoAtomicType
{
    //~ Instance fields -------------------------------------------------------

    private final Class classForPrimitive;

    /**
     * Class used to hold value at runtime (same as classForPrimitive for NOT
     * NULL types).
     */
    private final Class classForValue;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPrimitiveType object.
     *
     * @param simpleType .
     * @param isNullable .
     * @param classForValue .
     */
    FarragoPrimitiveType(
        CwmSqlsimpleType simpleType,
        boolean isNullable,
        Class classForValue)
    {
        super(simpleType,isNullable);
        this.classForValue = classForValue;
        // REVIEW: I'd like to have a 'getPrimitiveClass' interface to
        // implement, rather than relying on the supertype being a
        // NullablePrimitive, but its a bit of a pain to do it that way, due to
        // the desire to make it a static method ..
        if (NullablePrimitive.class.isAssignableFrom(classForValue)) {
            try {
                classForPrimitive = NullablePrimitive.getPrimitiveClass(
                    classForValue);
            } catch (Exception ex) {
                throw Util.newInternal(ex);
            }
        } else {
            classForPrimitive = classForValue;
        }
        computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * override FarragoAtomicType
     * @return class representing corresponding Java primitive
     */
    public Class getClassForPrimitive()
    {
        return classForPrimitive;
    }

    /**
     * .
     *
     * @return class used to hold value at runtime (same as
     * getClassForPrimitive() for NOT NULL types)
     */
    public Class getClassForValue()
    {
        return classForValue;
    }

    // implement FarragoType
    protected OJClass getOjClass(OJClass declarer)
    {
        return OJClass.forClass(classForValue);
    }

    // implement FarragoAtomicType
    public boolean hasClassForPrimitive()
    {
        return true;
    }

    // implement FarragoAtomicType
    public boolean requiresValueAccess()
    {
        return isNullable();
    }

}


// End FarragoPrimitiveType.java
