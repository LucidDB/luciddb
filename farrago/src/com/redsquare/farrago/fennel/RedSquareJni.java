/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 Red Square
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
package com.redsquare.farrago.fennel;

import org.eigenbase.util.*;

/**
 * JNI interface for Red Square extensions to Fennel which are used by
 * Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class RedSquareJni
{
    static {
        Util.loadLibrary("farrago_rs");
    }

    public static native void registerStreamFactory(long hStreamGraph);
}

// End RedSquareJni.java
