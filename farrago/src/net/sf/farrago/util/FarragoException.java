/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.util;


// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Farrago code.
import java.util.logging.*;


/**
 * Base class for all exceptions originating from Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoException extends RuntimeException
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        Logger.getLogger(FarragoException.class.getName());

    private int posLine;

    private int posColumn;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public FarragoException(
        String message,
        Throwable cause)
    {
        super(message, cause);

        // TODO: Force the caller to pass in a Logger as a trace argument for
        // better context.  Need to extend MonRG for this.
        tracer.throwing("FarragoException", "constructor", this);
        tracer.severe(toString());
    }

    /**
     * Sets a textual position at which this exception was detected.
     *
     * @param 1-based line number
     *
     * @param 1-based column number
     */
    public void setPosition(int posLine, int posColumn)
    {
        this.posLine = posLine;
        this.posColumn = posColumn;
    }

    /**
     * @return 1-based line number, or 0 for missing position information
     */
    public int getPosLine()
    {
        return posLine;
    }

    /**
     * @return 1-based column number, or 0 for missing position information
     */
    public int getPosColumn()
    {
        return posColumn;
    }
}


// End FarragoException.java
