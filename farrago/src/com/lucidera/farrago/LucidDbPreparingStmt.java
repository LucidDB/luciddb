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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

/**
 * LucidDbPreparingStmt refines {@link FarragoPreparingStmt} with
 * LucidDB-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbPreparingStmt extends FarragoPreparingStmt
{
    public LucidDbPreparingStmt(FarragoSessionStmtValidator stmtValidator)
    {
        super(stmtValidator);
    }

    // override FarragoPreparingStmt
    public boolean mayCacheImplementation()
    {
        // Do not cache statements which reference application variables,
        // since their values may change at any time (LER-2133).
        // REVIEW jvs 30-Sept-2006:  What about re-execution of a
        // PreparedStatement?  Also, should allow this as part of
        // DDL (similar to [NOT] DETERMINISTIC).

        for (CwmModelElement element : allDependencies) {
            if (!(element instanceof FemRoutine)) {
                continue;
            }
            if (!element.getName().equals("GET_VAR")) {
                continue;
            }
            if (element.getNamespace() == null) {
                continue;
            }
            if (!element.getNamespace().getName().equals("APPLIB")) {
                continue;
            }
            return false;
        }
        
        return super.mayCacheImplementation();
    }
}

// End LucidDbPreparingStmt.java
