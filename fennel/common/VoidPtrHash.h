/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_VoidPtrHash_Included
#define Fennel_VoidPtrHash_Included

FENNEL_BEGIN_NAMESPACE

/**
 * VoidPtrHash can be used to create a hash_map or hash_set with pointers as
 * keys.
 */
struct VoidPtrHash
{
    size_t operator() (void *key) const
    {
        return reinterpret_cast<size_t>(key);
    }
};

FENNEL_END_NAMESPACE

#endif

// End VoidPtrHash.h
