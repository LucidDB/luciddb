/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.defimpl;

import com.disruptivetech.farrago.fennel.*;
import com.redsquare.farrago.fennel.*;

import java.util.*;

import net.sf.farrago.db.*;
import net.sf.farrago.session.*;

/**
 * FarragoDefaultSession is a default implementation of the
 * {@link FarragoSession} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDefaultSession extends FarragoDbSession
{
    protected FarragoDefaultSession(
        String url,
        Properties info,
        FarragoSessionFactory sessionFactory)
    {
        super(url,info,sessionFactory);
    }

    // implement FarragoSession
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init)
    {
        FarragoDefaultPlanner planner = new FarragoDefaultPlanner(stmt);
        if (init) {
            planner.init();
        }
        return planner;
    }

    // implement FarragoSession
    public void registerStreamFactories(long hStreamGraph)
    {
        DisruptiveTechJni.registerStreamFactory(hStreamGraph);
        RedSquareJni.registerStreamFactory(hStreamGraph);
    }
}

// End FarragoDefaultSession.java
