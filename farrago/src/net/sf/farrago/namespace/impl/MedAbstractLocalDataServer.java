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

package net.sf.farrago.namespace.impl;

import net.sf.farrago.namespace.*;
import net.sf.farrago.fennel.*;

import java.util.*;

/**
 * MedAbstractLocalDataServer is an abstract base class for
 * implementations of the {@link FarragoMedLocalDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractLocalDataServer
    extends MedAbstractDataServer
    implements FarragoMedLocalDataServer
{
    private FennelDbHandle fennelDbHandle;

    protected MedAbstractLocalDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId,props);
    }

    /**
     * @return the Fennel database handle to use for accessing local storage
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    // implement FarragoMedLocalDataServer
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
    }
}

// End MedAbstractLocalDataServer.java
