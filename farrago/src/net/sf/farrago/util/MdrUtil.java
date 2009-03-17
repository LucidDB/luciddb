/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.util;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.enki.netbeans.*;


// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have no dependencies on other Farrago code.

/**
 * Static MDR utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MdrUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Loads an MDRepository instance.
     *
     * @param storageFactoryClassName fully qualified name of the class used to
     * implement repository storage (if null, this defaults to BtreeFactory if
     * not specified as an entry in storageProps)
     * @param storageProps storage-specific properties (with or without the
     * MDRStorageProperty prefix)
     *
     * @return loaded repository
     */
    public static EnkiMDRepository loadRepository(
        String storageFactoryClassName,
        Properties storageProps)
    {
        String classNameProp =
            "org.netbeans.mdr.storagemodel.StorageFactoryClassName";

        if (storageFactoryClassName != null) {
            storageProps.put(classNameProp, storageFactoryClassName);
        }

        EnkiMDRepository repos =
            MDRepositoryFactory.newMDRepository(storageProps);

        return repos;
    }

    /**
     * Integrates MDR tracing with Farrago tracing. Must be called before first
     * usage of MDR.
     *
     * @param mdrTracer Logger for MDR tracing
     */
    public static void integrateTracing(Logger mdrTracer)
    {
        MdrTraceUtil.integrateTracing(mdrTracer);
    }
}

// End MdrUtil.java
