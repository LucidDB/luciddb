/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_BTreeSearchExecStream_Included
#define Fennel_BTreeSearchExecStream_Included

#include "fennel/ftrs/BTreeReadExecStream.h"
#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeSearchExecStreamParams defines parameters for instantiating a
 * BTreeSearchExecStream.
 */
struct BTreeSearchExecStreamParams
    : public BTreeReadExecStreamParams, public ConduitExecStreamParams
{
    /**
     * When true, make up nulls for unmatched rows.
     */
    bool outerJoin;

    /**
     * Projection of attributes to be used as key from input stream.  If empty,
     * use the entire input stream as key.
     */
    TupleProjection inputKeyProj;

    /**
     * Projection of input attributes to be joined to search results in output.
     */
    TupleProjection inputJoinProj;
};

/**
 * BTreeSearchExecStream reads keys from a child and returns matching tuples in
 * the BTree.  Optionally, values from the input may also be joined to the
 * output (in which case they come before the values read from the BTree).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeSearchExecStream
    : public BTreeReadExecStream, public ConduitExecStream
{
protected:
    TupleProjectionAccessor inputKeyAccessor;
    TupleProjectionAccessor inputJoinAccessor;
    TupleProjectionAccessor readerKeyAccessor;
    TupleDescriptor inputKeyDesc;
    TupleData inputKeyData,readerKeyData;
    bool outerJoin;
    bool preFilterNulls;
    uint nJoinAttributes;

    bool innerSearchLoop();
    
    virtual void closeImpl();
    
public:
    // implement ExecStream
    void prepare(BTreeSearchExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeSearchExecStream.h
