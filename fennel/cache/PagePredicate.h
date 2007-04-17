/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_PagePredicate_Included
#define Fennel_PagePredicate_Included

#include "fennel/common/CompoundId.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_NAMESPACE

class MappedPageListener;

/**
 * Callback class for Cache::checkpointPages.
 */
class PagePredicate
{
public:
    virtual ~PagePredicate();
    
    /**
     * Tests the predicate.
     *
     * @param page the page to be considered
     *
     * @return true iff the page satisfies the predicate
     */
    virtual bool operator ()(CachePage const &page) = 0;
};

/**
 * DeviceIdPagePredicate is an implementation of PagePredicate which returns
 * true for pages mapped to a given DeviceId.
 */
class DeviceIdPagePredicate : public PagePredicate
{
    DeviceId deviceId;
    
public:
    explicit DeviceIdPagePredicate(DeviceId);
    
    virtual bool operator()(CachePage const &page);
};

/**
 * MappedPageListenerPredicate is an implementation of PagePredicate which
 * returns true for pages with a given MappedPageListener
 */
class MappedPageListenerPredicate : public PagePredicate
{
    MappedPageListener &listener;
    
public:
    explicit MappedPageListenerPredicate(MappedPageListener &);
    
    virtual bool operator()(CachePage const &page);
};

FENNEL_END_NAMESPACE

#endif

// End PagePredicate.h
