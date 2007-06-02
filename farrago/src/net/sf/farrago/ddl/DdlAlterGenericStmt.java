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
package net.sf.farrago.ddl;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;


/**
 * DdlAlterGenericStmt is a generic concrete extension of DdlAlterStmt.
 *
 * @author Steve Herskovitz
 * @version $Id$
 * @since May 26, 2006
 */
public class DdlAlterGenericStmt
    extends DdlAlterStmt
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlAlterGenericStmt.
     *
     * @param alterElement top-level element altered by this stmt
     */
    public DdlAlterGenericStmt(CwmModelElement alterElement)
    {
        super(alterElement);
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlAlterStmt
    protected void execute(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
    }
}

// End DdlAlterGenericStmt.java
