/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.fennel;

import java.nio.*;


/**
 * FennelJavaErrorTarget represents a class of java objects that can handle row
 * errors arising from Fennel streams.
 *
 * @author John Pham
 * @version $Id$
 */
public interface FennelJavaErrorTarget
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Handles a Fennel row exception
     *
     * @param source the unique Fennel stream name
     * @param isWarning true if the exception is only a warning
     * @param msg the exception string
     * @param byteBuffer the Fennel format byte buffer containing an error
     * record for the row that failed. The error record must conform to the row
     * type specified for the source with {@link
     * net.sf.farrago.query.FennelRelImplementor#setErrorRecordType}
     * @param index position of the column whose processing caused the exception
     * to occur. -1 indicates that no column was culpable. 0 indicates that a
     * filter condition was being processed. Otherwise this parameter should be
     * a 1-indexed column position.
     */
    public Object handleRowError(
        String source,
        boolean isWarning,
        String msg,
        ByteBuffer byteBuffer,
        int index);
}

// End FennelJavaErrorTarget.java
