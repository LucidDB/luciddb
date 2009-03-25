/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
package com.lucidera.lurql;

import org.netbeans.api.mdr.*;

/**
 * This package is obsolete and scheduled for deletion.
 *
 * @author John V. Sichi
 * @version $Id$
 *
 * @deprecated use org.eigenbase.lurql.LurqlQueryProcessor instead
 */
public class LurqlQueryProcessor
    extends org.eigenbase.lurql.LurqlQueryProcessor
{
    /**
     * Constructs a new LurqlQueryProcessor.
     */
    public LurqlQueryProcessor(MDRepository repos)
    {
        super(repos);
    }
}

// End LurqlQueryProcessor.java
