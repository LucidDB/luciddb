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

#ifndef Fennel_BTreeScan_Included
#define Fennel_BTreeScan_Included

#include "fennel/xo/BTreeReadTupleStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeScanParams defines parameters for instantiating a BTreeScan.
 */
struct BTreeScanParams : public BTreeReadTupleStreamParams
{
};

/**
 * BTreeScan reads all data from a BTree.
 */
class BTreeScan : public BTreeReadTupleStream
{
public:
    void prepare(BTreeScanParams const &params);
    virtual void open(bool restart);
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeScan.h
