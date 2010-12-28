/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package net.sf.farrago.fennel.rel;

import java.math.*;


/**
 * FennelSearchEndpoint defines an enumeration corresponding to
 * fennel/common/SearchEndpoint.h. Any changes there must be applied here as
 * well.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum FennelSearchEndpoint
{
    /**
     * Defines the beginning of an interval which is unbounded below. The
     * associated key value should be all null.
     */
    SEARCH_UNBOUNDED_LOWER('-'),

    /**
     * Defines the beginning of an interval which has an open bound below.
     */
    SEARCH_OPEN_LOWER('('),

    /**
     * Defines the beginning of an interval which has a closed bound below.
     */
    SEARCH_CLOSED_LOWER('['),

    /**
     * Defines the end of an interval which has an open bound above.
     */
    SEARCH_OPEN_UPPER(')'),

    /**
     * Defines the end of an interval which has a closed bound above.
     */
    SEARCH_CLOSED_UPPER(']'),

    /**
     * Defines the end of an interval which is unbounded above. The associated
     * key value should be all null.
     */
    SEARCH_UNBOUNDED_UPPER('+');

    private final String symbol;

    private FennelSearchEndpoint(char symbol)
    {
        this.symbol = new String(new char[] { symbol });
    }

    /**
     * @return symbol used to communicate endpoint type to Fennel
     */
    public String getSymbol()
    {
        return symbol;
    }
}

// End FennelSearchEndpoint.java
