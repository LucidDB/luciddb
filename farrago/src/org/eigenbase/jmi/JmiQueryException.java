/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.jmi;

/**
 * JmiQueryException specifies an exception thrown during JMI query processing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiQueryException
    extends Exception
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new exception.
     *
     * @param message description of exception
     */
    public JmiQueryException(
        String message)
    {
        this(message, null);
    }

    /**
     * Constructs a new exception with an underlying cause.
     *
     * @param message description of exception
     * @param cause underlying cause
     */
    public JmiQueryException(
        String message,
        Throwable cause)
    {
        super(message, cause);
    }
}

// End JmiQueryException.java
