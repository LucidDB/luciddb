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

import java.lang.reflect.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetSessionParamStmt represents the ALTER SESSION SET ... statement.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlSetSessionParamStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    final private String paramName;
    final private SqlLiteral paramValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetSessionParamStmt.
     *
     * @param paramName name of parameter to set
     * @param paramValue new value for parameter
     */
    public DdlSetSessionParamStmt(
        String paramName,
        SqlLiteral paramValue)
    {
        super(null);
        this.paramName = paramName;
        this.paramValue = paramValue;
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoSession session = ddlValidator.getInvokingSession();
        String valueString = paramValue.toValue();

        session.getPersonality().validateSessionVariable(
            ddlValidator,
            session.getSessionVariables(),
            paramName,
            valueString);
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}

// End DdlSetSystemParam.java
