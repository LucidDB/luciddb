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
package com.lucidera.luciddb.applib.resource;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase code.
import java.util.logging.*;
import org.eigenbase.util.EigenbaseException;

/**
 * Exception class for Applib
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ApplibException extends EigenbaseException
{
    private static Logger tracer =
        Logger.getLogger(ApplibException.class.getName());

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
