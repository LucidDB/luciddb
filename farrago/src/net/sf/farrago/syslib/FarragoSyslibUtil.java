/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 SQLstream, Inc.
// Copyright (C) 2008-2008 LucidEra, Inc.
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
package net.sf.farrago.syslib;

import java.sql.*;

import java.util.*;

import net.sf.farrago.type.runtime.*;

import org.eigenbase.util.*;


/**
 * FarragoSyslibUtil provides utility methods useful when constructing UDR's.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FarragoSyslibUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Compares two key objects retrieved from cursor columns having the exact
     * same SQL datatype. Attempting to compare objects retrieved from columns
     * with different datatypes may result in assertion violations or incorrect
     * results; likewise for attempts to compare objects not retrieved from
     * cursors. The comparison semantics used are the same as for GROUP BY:
     *
     * <ul>
     * <li>NULL matches NULL
     * <li>non-NULL does not match NULL
     * <li>character data is right-trimmed before comparison
     * </ul>
     *
     * NOTE jvs 19-Apr-2008: once we support character set collations, this will
     * not be good enough
     *
     * <p>Some unit tests are in farrago/unitsql/syslib/presort.sql
     *
     * @param obj1 first key to compare
     * @param obj2 second key to compare
     *
     * @return negative for obj1 < obj2; positive for obj1 > obj2; zero for obj1
     * == obj2
     */
    public static int compareKeysUsingGroupBySemantics(
        Object obj1,
        Object obj2)
    {
        // This covers null == null
        if (obj1 == obj2) {
            return 0;
        }
        if (obj1 == null) {
            // we know obj2 is not null, so obj1 < obj2
            return -1;
        }
        if (obj2 == null) {
            // we know obj1 is not null, so obj1 > obj2
            return 1;
        }
        if (obj1 instanceof String) {
            assert (obj2 instanceof String) : obj2.getClass().getName();
            String s1 = (String) obj1;
            String s2 = (String) obj2;
            return Util.rtrim(s1).compareTo(Util.rtrim(s2));
        } else if (obj1 instanceof byte []) {
            assert (obj2 instanceof byte []) : obj2.getClass().getName();
            byte [] b1 = (byte []) obj1;
            byte [] b2 = (byte []) obj2;

            // TODO jvs 19-Apr-2008:  optimize
            BytePointer bp1 = new BytePointer();
            BytePointer bp2 = new BytePointer();
            bp1.setPointer(b1, 0, b1.length);
            bp2.setPointer(b2, 0, b2.length);
            return bp1.compareBytes(bp2);
        } else {
            assert (obj1 instanceof Comparable) : obj1.getClass().getName();
            assert (obj2 instanceof Comparable) : obj2.getClass().getName();
            Comparable c1 = (Comparable) obj1;
            Comparable c2 = (Comparable) obj2;
            return c1.compareTo(c2);
        }
    }
}

// End FarragoSyslibUtil.java
