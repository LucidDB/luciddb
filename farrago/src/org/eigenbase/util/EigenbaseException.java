/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.util;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase code.
import java.util.logging.*;


/**
 * Base class for all exceptions originating from Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 * @see EigenbaseContextException
 */
public class EigenbaseException
    extends RuntimeException
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool. Prevents
     * incompatible class conflict when serialized from JDK 1.5-built server to
     * JDK 1.4-built client.
     */
    private static final long serialVersionUID = -1314522633397794178L;

    private static Logger tracer =
        Logger.getLogger(EigenbaseException.class.getName());

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new EigenbaseException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public EigenbaseException(
        String message,
        Throwable cause)
    {
        super(message, cause);

        // TODO: Force the caller to pass in a Logger as a trace argument for
        // better context.  Need to extend ResGen for this.
        tracer.throwing("EigenbaseException", "constructor", this);
        tracer.severe(toString());
    }
}

// End EigenbaseException.java
