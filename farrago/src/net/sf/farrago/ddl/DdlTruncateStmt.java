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
package net.sf.farrago.ddl;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;


/**
 * DdlTruncateStmt represents a DDL TRUNCATE statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlTruncateStmt
    extends DdlStmt
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlTruncateStmt.
     *
     * @param truncatedElement top-level element truncated by this stmt
     */
    public DdlTruncateStmt(CwmModelElement truncatedElement)
    {
        super(truncatedElement);
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);

        // There's no JMI operation corresponding to a truncation, so use
        // an explicit call to request it.
        ddlValidator.scheduleTruncation(getModelElement());
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}

// End DdlTruncateStmt.java
