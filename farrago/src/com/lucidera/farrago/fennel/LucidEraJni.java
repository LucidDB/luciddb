/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2004-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.lucidera.farrago.fennel;

import org.eigenbase.util.*;


/**
 * JNI interface for LucidEra extensions to Fennel which are used by Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LucidEraJni
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        Util.loadLibrary("farrago_lu");
    }

    //~ Methods ----------------------------------------------------------------

    public static native void registerStreamFactory(long hStreamGraph);
}

// End LucidEraJni.java
