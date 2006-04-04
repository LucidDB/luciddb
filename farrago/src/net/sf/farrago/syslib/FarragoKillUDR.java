/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.syslib;

import java.sql.SQLException;
import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.db.FarragoDatabase;
import net.sf.farrago.db.FarragoDbSession;
import net.sf.farrago.runtime.FarragoUdrRuntime;

/**
 * FarragoKillUDR defines some system procedures for killing sessions and executing statements.
 * (Technically these are user-defined procedures.)
 * They are intended for use by a system administrator, and are installed by
 * initsdl/createSyslibSchema.sql.
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public abstract class FarragoKillUDR
{
    /** 
     * Kills a running session
     * @param id unique session identifier: long or Long or int??
     */
    public static void killSession(long id) throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killSession(id);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }

    /** 
     * Kills an executing statement.
     * @param id unique statement identifier
     */
    public static void killStatement(long id) throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killExecutingStmt(id);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }
}
