/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.type.runtime;

/**
 * SpecialDataValue is an interface representing a runtime holder for a data
 * value. It may be used as an alternative to {@link DataValue}.
 *
 * <p>Since DataValue is typically used return Jdbc data, this class may be used
 * to return non-Jdbc data.
 *
 * @version $Id$
 */
public interface SpecialDataValue
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return an Object representation of this value's data, or null if this
     * value is null
     */
    Object getSpecialData();
}

// End SpecialDataValue.java
