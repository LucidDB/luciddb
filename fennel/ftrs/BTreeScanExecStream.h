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

#ifndef Fennel_BTreeScanExecStream_Included
#define Fennel_BTreeScanExecStream_Included

#include "fennel/ftrs/BTreeReadExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeScanExecStreamParams defines parameters for instantiating a
 * BTreeScanExecStream.
 */
struct BTreeScanExecStreamParams : public BTreeReadExecStreamParams
{
};

/**
 * BTreeScanExecStream reads all data from a BTree.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeScanExecStream : public BTreeReadExecStream
{
public:
    // implement ExecStream
    virtual void prepare(BTreeScanExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeScanExecStream.h
