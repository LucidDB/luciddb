/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

#ifndef Fennel_SearchEndpoint_Included
#define Fennel_SearchEndpoint_Included

FENNEL_BEGIN_NAMESPACE

/**
 * SearchEndpoint defines directives used to control a search
 * in an ordered associative data structure such as a BTree.
 * An endpoint is associated with a particular key value.
 *
 *<p>
 *
 * NOTE jvs 23-Jan-2006:  any changes made here must be
 * applied to net.sf.farrago.query.FennelSearchEndpoint as well.
 *
 * @author John V. Sichi
 * @version $Id$
 */
enum SearchEndpoint
{
    /**
     * Defines the beginning of an interval which is unbounded below.  The
     * associated key value should be all null.
     */
    SEARCH_UNBOUNDED_LOWER = '-',

    /**
     * Defines the beginning of an interval which has an open bound below.
     */
    SEARCH_OPEN_LOWER = '(',

    /**
     * Defines the beginning of an interval which has a closed bound below.
     */
    SEARCH_CLOSED_LOWER = '[',

    /**
     * Defines the end of an interval which has an open bound above.
     */
    SEARCH_OPEN_UPPER = ')',

    /**
     * Defines the end of an interval which has a closed bound above.
     */
    SEARCH_CLOSED_UPPER = ']',

    /**
     * Defines the end of an interval which is unbounded above.  The associated
     * key value should be all null.
     */
    SEARCH_UNBOUNDED_UPPER = '+',
};

FENNEL_END_NAMESPACE

#endif

// End SearchEndpoint.h
