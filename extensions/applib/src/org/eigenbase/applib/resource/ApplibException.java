/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.resource;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase code.
import java.util.logging.*;

import org.eigenbase.util.*;


/**
 * Exception class for Applib
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ApplibException
    extends EigenbaseException
{
    //~ Static fields/initializers ---------------------------------------------

    private static Logger tracer =
        Logger.getLogger(ApplibException.class.getName());

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new ApplibException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public ApplibException(String message, Throwable cause)
    {
        super(message, cause);

        // TODO: Force the caller to pass in a Logger as a trace argument for
        // better context.  Need to extend ResGen for this.
        //tracer.throwing("ApplibException", "constructor", this);
        //tracer.severe(toString());
    }
}

// End ApplibException.java
