/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_Distinctness_Included
#define Fennel_Distinctness_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Options for how to deal with insertion of a duplicate key.
 */
enum Distinctness
{
    /**
     * Allow duplicates.
     */
    DUP_ALLOW,
    /**
     * Discard duplicates.
     */
    DUP_DISCARD,
    /**
     * Fail when duplicates are encountered.
     */
    DUP_FAIL
};

/**
 * Options for how to deal with detection of a duplicate key while
 * searching.
 */
enum DuplicateSeek
{
    /**
     * Position to an arbitrary match for the duplicate key.
     */
    DUP_SEEK_ANY,

    /**
     * Position to the first match for a duplicate key.
     */
    DUP_SEEK_BEGIN,

    /**
     * Position to one past the last match for a duplicate key.
     */
    DUP_SEEK_END
};

FENNEL_END_NAMESPACE

#endif

// End Distinctness.h
