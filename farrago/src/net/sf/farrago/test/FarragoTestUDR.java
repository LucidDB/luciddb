/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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
package net.sf.farrago.test;

/**
 * FarragoTestUDR contains definitions for user-defined routines used
 * by tests.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoTestUDR
{
    public static String noargs()
    {
        return "get your kicks on route 66";
    }

    public static String substring24(String in)
    {
        return in.substring(2, 4);
    }

    public static String toHexString(int i)
    {
        return Integer.toHexString(i);
    }
    
    public static String toHexString(Integer i)
    {
        if (i == null) {
            return "nada";
        } else {
            return Integer.toHexString(i.intValue());
        }
    }

    public static int atoi(String s)
    {
        return Integer.parseInt(s);
    }

    public static Integer atoiWithNullForErr(String s)
    {
        try {
            return new Integer(Integer.parseInt(s));
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}

// End FarragoTestUDR.java
