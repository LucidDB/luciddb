/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.ddl;

import net.sf.farrago.session.*;
import net.sf.farrago.cwm.core.*;

/**
 * DdlTruncateStmt represents a DDL TRUNCATE statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlTruncateStmt extends DdlStmt
{
    /**
     * Construct a new DdlTruncateStmt.
     *
     * @param truncatedElement top-level element truncated by this stmt
     */
    public DdlTruncateStmt(CwmModelElement truncatedElement)
    {
        super(truncatedElement);
    }
    
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
