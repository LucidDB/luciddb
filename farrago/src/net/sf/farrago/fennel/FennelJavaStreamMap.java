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
package net.sf.farrago.fennel;


/**
 * FennelJavaStreamMap is needed when a Fennel TupleStream's definition includes
 * calls to JavaTupleStreams.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelJavaStreamMap
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Looks up the handle of a JavaTupleStream by its ID.  This is called by
     * native code when a TupleStream is opened.  The ID is a placeholder in
     * the TupleStream definition; each open may result in a different handle.
     *
     *
     * @param streamId ID of stream to find
     *
     * @return JavaTupleStream handle
     */
    public long getJavaStreamHandle(int streamId);

    /**
     * Looks up the root PageId of an index.  This is called by
     * native code when a TupleStream accessing a temporary BTree is opened.
     *
     * @param pageOwnerId the identifier for the index
     */
    public long getIndexRoot(long pageOwnerId);
}


// End FennelJavaStreamMap.java
