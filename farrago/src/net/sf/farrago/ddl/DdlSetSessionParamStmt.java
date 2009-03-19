/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


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
    private FemLabel labelParamValue;

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
        this.labelParamValue = null;
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

        // Retrieve the underlying label object
        if (paramName.equals(FarragoDefaultSessionPersonality.LABEL)) {
            // Labels can't be set inside UDR's because UDR's currently
            // run as a single transaction initiated by the caller of the
            // UDR.  So setting the label will be a  no-op.
            if (FarragoUdrRuntime.inUdr()) {
                throw FarragoResource.instance().ValidatorSetLabelInUdr.ex();
            }
            if (valueString != null) {
                SqlIdentifier unQualifiedName =
                    new SqlIdentifier(
                        valueString,
                        new SqlParserPos(0, 0));
                labelParamValue =
                    ddlValidator.getStmtValidator().findUnqualifiedObject(
                        unQualifiedName,
                        FemLabel.class);
            }
        }
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    /**
     * @return the name of the parameter being set
     */
    public String getParamName()
    {
        return paramName;
    }

    /**
     * @return the value of the parameter if the label parameter is being set
     */
    public FemLabel getLabelParamValue()
    {
        return labelParamValue;
    }
}

// End DdlSetSessionParamStmt.java
