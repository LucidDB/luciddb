/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_ExecutionStreamResourceQuantity_Included
#define Fennel_ExecutionStreamResourceQuantity_Included

FENNEL_BEGIN_NAMESPACE

/**
 * ExecutionStreamResourceQuantity quantifies various resources which
 * can be allocated to an ExecutionStream.
 */
struct ExecutionStreamResourceQuantity
{
    /**
     * Number of dedicated threads the stream may request while executing.
     * Non-parallelized streams have 0 for this setting, meaning the only
     * threads which execute them are managed by the scheduler instead.
     */
    uint nThreads;

    /**
     * Number of cache pages the stream may pin while executing.  This includes
     * both scratch pages and I/O pages used for storage access.
     */
    uint nCachePages;

    explicit ExecutionStreamResourceQuantity()
    {
        nThreads = 0;
        nCachePages = 0;
    }
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamResourceQuantity.h
