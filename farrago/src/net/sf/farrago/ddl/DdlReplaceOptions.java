/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.ddl;

import org.eigenbase.sql.*;


/**
 * DdlReplaceOptions contains attributes of a CREATE OR REPLACE statement.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class DdlReplaceOptions
{
    //~ Instance fields --------------------------------------------------------

    private boolean isReplace;
    private SqlIdentifier newName;

    //~ Constructors -----------------------------------------------------------

    public DdlReplaceOptions()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isReplace()
    {
        return isReplace;
    }

    public SqlIdentifier getNewName()
    {
        return newName;
    }

    public void setIsReplace(boolean isReplace)
    {
        this.isReplace = isReplace;
    }

    public void setNewName(SqlIdentifier newName)
    {
        this.newName = newName;
    }
}

// End DdlReplaceOptions.java
