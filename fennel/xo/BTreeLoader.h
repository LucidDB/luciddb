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

#ifndef Fennel_BTreeLoader_Included
#define Fennel_BTreeLoader_Included

#include "fennel/xo/BTreeTupleStream.h"
#include "fennel/xo/DoubleInputTupleStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeLoaderParams defines parameters for instantiating a BTreeLoader.
 */
struct BTreeLoaderParams : public BTreeStreamParams
{
    Distinctness distinctness;

    // REVIEW:  should this be a SegmentAccessor instead?
    SharedSegment pTempSegment;
};

/**
 * BTreeLoader loads presorted tuples read from a child stream into a BTree.
 * Its first child stream provides an exact count of the number of tuples to
 * expect.  Its second child stream provides the actual tuple data.
 */
class BTreeLoader : public BTreeTupleStream, public DoubleInputTupleStream
{
    Distinctness distinctness;
    TupleDescriptor zeroTupleDesc;
    SharedBTreeBuilder pBuilder;
    SharedSegment pTempSegment;
    
    virtual void closeImpl();
    
    void executeInsertion();
    
public:
    void prepare(BTreeLoaderParams const &params);
    virtual void open(bool restart);
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual BufferProvision getInputBufferRequirement() const;
};

FENNEL_END_NAMESPACE

#endif

// End BTreeLoader.h
