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

import java.util.*;

/**
 * A comparator for two BytePointer objects interpreted as VARBINARY.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class VarbinaryComparator implements Comparator
{
    public static final int compareVarbinary(Object o1,Object o2)
    {
        BytePointer bp1 = (BytePointer) o1;
        BytePointer bp2 = (BytePointer) o2;
        return bp1.compareBytes(bp2);
    }
    
    public int compare(Object o1,Object o2)
    {
        return compareVarbinary(o1,o2);
    }
}

// End VarbinaryComparator.java
