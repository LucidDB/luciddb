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

#ifndef Fennel_BTreeInsertExecStream_Included
#define Fennel_BTreeInsertExecStream_Included

#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeInserterParams defines parameters for instantiating a BTreeInserter.
 */
struct BTreeInsertExecStreamParams
    : public BTreeExecStreamParams, public ConduitExecStreamParams
{
    Distinctness distinctness;
};

/**
 * BTreeInsertExecStream inserts tuples into a BTree, reading them from an
 * upstream stream producer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeInsertExecStream : public BTreeExecStream, public ConduitExecStream
{
    Distinctness distinctness;
    
protected:
    SharedBTreeWriter pWriter;
    
    virtual void closeImpl();
    
public:
    // implement ExecStream
    virtual void prepare(BTreeInsertExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeInsertExecStream.h
