/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.catalog;

import javax.jmi.reflect.*;


/**
 * FarragoReposIntegrityErr records one integrity error detected by {@link
 * FarragoRepos#verifyIntegrity}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReposIntegrityErr
{
    //~ Instance fields --------------------------------------------------------

    private final String description;

    private final JmiException exception;

    private final RefObject refObj;

    //~ Constructors -----------------------------------------------------------

    public FarragoReposIntegrityErr(
        String description,
        JmiException exception,
        RefObject refObj)
    {
        this.description = description;
        this.exception = exception;
        this.refObj = refObj;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return description of the error
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * @return underlying exception reported by JMI, or null if failed integrity
     * rule was specific to Farrago
     */
    public JmiException getJmiException()
    {
        return exception;
    }

    /**
     * @return object on which error was detected, or null if error is not
     * specific to an object
     */
    public RefObject getRefObject()
    {
        return refObj;
    }

    // implement Object
    public String toString()
    {
        return description;
    }
}

// End FarragoReposIntegrityErr.java
