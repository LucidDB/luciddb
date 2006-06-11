/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.farrago;

import net.sf.farrago.server.*;

import java.io.*;

/**
 * LucidDbServer is a wrapper to insulate LucidDB scripts from direct
 * dependencies on Farrago classes.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbServer extends FarragoVjdbcServer
{
    public LucidDbServer(PrintWriter pw)
        throws Exception
    {
        super(pw);
    }
}

// End LucidDbServer.java
