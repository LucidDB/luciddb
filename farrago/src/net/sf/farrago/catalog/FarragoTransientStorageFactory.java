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
package net.sf.farrago.catalog;

import java.util.*;

import org.netbeans.mdr.persistence.*;
import org.netbeans.mdr.persistence.memoryimpl.*;


/**
 * Factory for {@link FarragoTransientStorage}. Adapted from
 * org.netbeans.mdr.persistence.memoryimpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTransientStorageFactory
    implements StorageFactory
{
    //~ Static fields/initializers ---------------------------------------------

    // distinguish this from normal memory storage
    static final String NULL_STORAGE_ID = "#";

    private static final MOFID NULL_MOFID = new MOFID(0, NULL_STORAGE_ID);
    private static FarragoTransientStorage singletonStorage;

    //~ Constructors -----------------------------------------------------------

    public FarragoTransientStorageFactory()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement StorageFactory
    public synchronized Storage createStorage(Map properties)
        throws StorageException
    {
        singletonStorage = new FarragoTransientStorage();
        return singletonStorage;
    }

    // implement StorageFactory
    public MOFID createNullMOFID()
        throws StorageException
    {
        return NULL_MOFID;
    }
}

// End FarragoTransientStorageFactory.java
