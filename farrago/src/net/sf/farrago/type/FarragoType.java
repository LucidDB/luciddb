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
import net.sf.farrago.resource.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;

import openjava.mop.*;


/**
 * FarragoType is the abstract superclass for all Farrago-specific
 * implementations of the SaffronType interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoType implements SaffronType
{
    //~ Instance fields -------------------------------------------------------

    /** Computed digest. */
    protected String digest;

    /** Owning FarragoTypeFactory. */
    FarragoTypeFactory factory;

    //~ Methods ---------------------------------------------------------------

    // implement SaffronType
    public SaffronTypeFactory getFactory()
    {
        return factory;
    }

    // implement SaffronType
    public boolean isJoin()
    {
        return false;
    }

    // implement SaffronType
    public SaffronType [] getJoinTypes()
    {
        assert (isJoin());
        throw Util.newInternal("not reached");
    }

    // implement SaffronType
    public boolean isProject()
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

    // implement SaffronType
    public String toString()
    {
        return digest;
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

    /**
     * Forget original factory so that it can be garbage collected.
     */
    public void forgetFactory()
    {
        factory = null;
        digest = null;
    }
}


// End FarragoType.java
