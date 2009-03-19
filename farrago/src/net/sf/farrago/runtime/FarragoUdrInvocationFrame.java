/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import java.sql.*;

import net.sf.farrago.session.*;

import org.eigenbase.enki.mdr.*;


/**
 * FarragoUdrInvocationFrame represents one entry on the routine invocation
 * stack for a given thread.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoUdrInvocationFrame
{
    //~ Instance fields --------------------------------------------------------

    FarragoRuntimeContext context;

    EnkiMDSession reposSession;

    FarragoSessionUdrContext udrContext;

    boolean allowSql;

    Connection connection;
}

// End FarragoUdrInvocationFrame.java
