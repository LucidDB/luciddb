/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
