/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.util;

import java.util.logging.Logger;


// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Saffron code.

/**
 * Base class for all exceptions orginating from Saffron.
 *
 * @author klo
 * @since May 19, 2004
 * @version $Id$
 **/
public class SaffronException extends RuntimeException
{
    /**
     * Tracer which reports when a SaffronException is created.
     *
     * <p>{@link net.sf.saffron.trace.SaffronTrace} stipulates that tracers
     * should be private or protected in the class which uses them, and should
     * reference a public field in SaffronTrace. This class breaks that
     * stipulation, because it needs to be separately compileable, and so
     * cannot depend upon SaffronTrace.
     */
    public static final Logger tracer =
        Logger.getLogger(SaffronException.class.getName());

    /**
     * Creates a new SaffronException object
     * @param message error message
     * @param cause underlying cause
     */
    public SaffronException(
        String message,
        Throwable cause)
    {
        super(message, cause);

        tracer.throwing("SaffronException", "constructor", this);
        tracer.severe(toString());
    }
}


// End SaffronException.java
