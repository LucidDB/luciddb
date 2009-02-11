/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.session.*;

/**
 * DdlDropLabelStmt extends DdlDropStmt to remove obsolete label statistics.
 * Repository design constraints related to upgrade prevent the statistics 
 * from being associated with the label itself.  They are instead associated
 * implicitly via time stamps.  Therefore, drop rules are insufficient to 
 * trigger the necessary deletions.  Avoid this pattern whenever possible!
 * 
 * @author Stephan Zuercher
 */
public class DdlDropLabelStmt
    extends DdlDropStmt
{
    /**
     * Constructs a new DdlDropLabelStmt.
     *
     * @param droppedElement FemLabel dropped by this stmt
     * @param restrict whether DROP RESTRICT is in effect
     */
    public DdlDropLabelStmt(
        FemLabel droppedElement,
        boolean restrict)
    {
        super(droppedElement, restrict);
    }

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        // Remove stats associated with the label being dropped.  Note that
        // this needs to be done before the label is deleted from the
        // catalog.
        FarragoCatalogUtil.removeObsoleteStatistics(
            (FemLabel)getModelElement(), ddlValidator.getRepos());
        
        super.preValidate(ddlValidator);
    }
}
