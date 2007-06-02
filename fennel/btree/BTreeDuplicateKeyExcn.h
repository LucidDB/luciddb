/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_BTreeDuplicateKeyExcn_Included
#define Fennel_BTreeDuplicateKeyExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;

/**
 * Exception class for duplicate keys encountered during insert or update.
 */
class BTreeDuplicateKeyExcn : public FennelExcn
{
public:
    /**
     * Constructs a new BTreeDuplicateKeyExcn.
     *
     *<p>
     *
     * TODO:  take more information to get a better key description
     *
     * @param keyDescriptor TupleDescriptor for the BTree's key
     *
     * @param keyData data for the duplicate key
     */
    explicit BTreeDuplicateKeyExcn(
        TupleDescriptor const &keyDescriptor,
        TupleData const &keyData);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeDuplicateKeyExcn.h
