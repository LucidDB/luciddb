/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.HashMap;
import java.util.Map;


public class FarragoSessionInfo
{
    //~ Instance fields -------------------------------------------------------

    private Map<Integer, FarragoSessionExecutingStmtInfo> statements;

    //~ Constructors ----------------------------------------------------------

    public FarragoSessionInfo()
    {
        statements = new HashMap<Integer, FarragoSessionExecutingStmtInfo>();
    }

    //~ Methods ---------------------------------------------------------------

    public Integer [] getExecutingStmtIds()
    {
        return statements.keySet().toArray(new Integer[statements.size()]);
    }

    public FarragoSessionExecutingStmtInfo getExecutingStmtInfo(Integer id)
    {
        return statements.get(id);
    }
    
    public void addExecutingStmtInfo(FarragoSessionExecutingStmtInfo info) {
        statements.put(info.getId(), info);
    }
    
    public void removeExecutingStmtInfo(Integer id) {
        statements.remove(id);
    }
}
