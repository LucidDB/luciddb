/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

/**
 * Exception which contains information about the textual context of the causing
 * exception.
 */
public class EigenbaseContextException
    extends EigenbaseException
{
    //~ Instance fields --------------------------------------------------------

    private int posLine;

    private int posColumn;

    private int endPosLine;

    private int endPosColumn;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new EigenbaseContextException object. This constructor is for
     * use by the generated factory.
     *
     * @param message error message
     * @param cause underlying cause, must not be null
     */
    public EigenbaseContextException(String message, Throwable cause)
    {
        this(message, cause, 0, 0, 0, 0);
    }

    /**
     * Creates a new EigenbaseContextException object.
     *
     * @param message error message
     * @param cause underlying cause, must not be null
     * @param posLine 1-based start line number
     * @param posColumn 1-based start column number
     * @param endPosLine 1-based end line number
     * @param endPosColumn 1-based end column number
     */
    public EigenbaseContextException(
        String message,
        Throwable cause,
        int posLine,
        int posColumn,
        int endPosLine,
        int endPosColumn)
    {
        super(message, cause);
        assert (cause != null);
        setPosition(posLine, posColumn, endPosLine, endPosColumn);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets a textual position at which this exception was detected.
     *
     * @param posLine 1-based line number
     * @param posColumn 1-based column number
     */
    public void setPosition(int posLine, int posColumn)
    {
        this.posLine = posLine;
        this.posColumn = posColumn;
        this.endPosLine = posLine;
        this.endPosColumn = posColumn;
    }

    /**
     * Sets a textual range at which this exception was detected.
     *
     * @param posLine 1-based start line number
     * @param posColumn 1-based start column number
     * @param endPosLine 1-based end line number
     * @param endPosColumn 1-based end column number
     */
    public void setPosition(
        int posLine,
        int posColumn,
        int endPosLine,
        int endPosColumn)
    {
        this.posLine = posLine;
        this.posColumn = posColumn;
        this.endPosLine = endPosLine;
        this.endPosColumn = endPosColumn;
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

    /**
     * @return 1-based ending line number, or 0 for missing position information
     */
    public int getEndPosLine()
    {
        return endPosLine;
    }

    /**
     * @return 1-based ending column number, or 0 for missing position
     * information
     */
    public int getEndPosColumn()
    {
        return endPosColumn;
    }
}

// End EigenbaseContextException.java
