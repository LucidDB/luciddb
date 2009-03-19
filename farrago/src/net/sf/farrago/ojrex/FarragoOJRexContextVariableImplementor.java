/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.ojrex;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.rex.*;


/**
 * Implements Farrago specifics of {@link OJRexImplementor} for context
 * variables such as "USER" and "SESSION_USER".
 *
 * <p>For example, "USER" becomes "connection.getContextVariable_USER()".
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoOJRexContextVariableImplementor
    implements OJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final String name;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexContextVariableImplementor(String name)
    {
        this.name = name;
    }

    //~ Methods ----------------------------------------------------------------

    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        FarragoRexToOJTranslator farragoTranslator =
            (FarragoRexToOJTranslator) translator;
        return farragoTranslator.convertVariable(
            call.getType(),
            "getContextVariable_" + name,
            new ExpressionList());
    }

    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexContextVariableImplementor.java
