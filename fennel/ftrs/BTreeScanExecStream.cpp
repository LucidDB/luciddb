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

#include "fennel/common/CommonPreamble.h"
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeScanExecStream::prepare(BTreeScanExecStreamParams const &params)
{
    BTreeReadExecStream::prepare(params);
}

void BTreeScanExecStream::open(bool restart)
{
    BTreeReadExecStream::open(restart);
    if (!pReader->searchFirst()) {
        pReader->endSearch();
    }
}

ExecStreamResult BTreeScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    // TODO: (under parameter control) unlock current leaf before return and
    // relock it on next fetch
    
    uint nTuples = 0;
    
    while (pReader->isPositioned()) {
        projAccessor.unmarshal(tupleData);
        if (pOutAccessor->produceTuple(tupleData)) {
            ++nTuples;
        } else {
            return EXECRC_BUF_OVERFLOW;
        }
        if (!pReader->searchNext()) {
            pReader->endSearch();
            break;
        }
        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
    }

    if (!nTuples) {
        pOutAccessor->markEOS();
    }
    return EXECRC_EOS;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeScanExecStream.cpp
