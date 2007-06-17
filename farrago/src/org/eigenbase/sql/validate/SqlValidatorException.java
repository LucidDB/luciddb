/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
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
package org.eigenbase.sql.validate;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase/Farrago code.
import java.util.logging.*;

import org.eigenbase.util14.*;


/**
 * Exception thrown while validating a SQL statement.
 *
 * <p>Unlike {@link org.eigenbase.util.EigenbaseException}, this is a checked
 * exception, which reminds code authors to wrap it in another exception
 * containing the line/column context.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 28, 2004
 */
public class SqlValidatorException
    extends Exception
    implements EigenbaseValidatorException
{
    //~ Static fields/initializers ---------------------------------------------

    private static Logger tracer =
        Logger.getLogger("org.eigenbase.util.EigenbaseException");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SqlValidatorException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public SqlValidatorException(
        String message,
        Throwable cause)
    {
        super(message, cause);

        // TODO: see note in EigenbaseException constructor
        tracer.throwing("SqlValidatorException", "constructor", this);
        tracer.severe(toString());
    }
}

// End SqlValidatorException.java
