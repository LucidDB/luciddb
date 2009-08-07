/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package net.sf.farrago.defimpl;

import net.sf.farrago.fennel.*;
import net.sf.farrago.session.*;
import net.sf.farrago.db.*;

import net.sf.farrago.fem.fennel.*;

import java.sql.*;

/**
 * FarragoSebSessionFactory enables the storage engine bridge.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoSebSessionFactory extends FarragoDefaultSessionFactory
{
    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        return new FarragoSebSessionPersonality((FarragoDbSession) session);
    }

    // implement FarragoSessionFactory
    public FennelCmdExecutor newFennelCmdExecutor()
    {
        return new FennelCmdExecutor()
            {
                public long executeJavaCmd(
                    FemCmd cmd, FennelExecutionHandle execHandle)
                    throws SQLException
                {
                    if (execHandle == null) {
                        return SebJni.executeJavaCmd(cmd, 0);
                    } else {
                        return SebJni.executeJavaCmd(
                            cmd, execHandle.getHandle());
                    }
                }
            };
    }

    public static class FarragoSebSessionPersonality
        extends FarragoDefaultSessionPersonality
    {
        public FarragoSebSessionPersonality(FarragoDbSession session)
        {
            super(session);
        }

        // implement FarragoStreamFactoryProvider
        public void registerStreamFactories(long hStreamGraph)
        {
            SebJni.registerStreamFactory(hStreamGraph);
        }
    }
}

// End FarragoSebSessionFactory.java
