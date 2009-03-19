/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

#ifndef Fennel_LbmEntryDump_Included
#define Fennel_LbmEntryDump_Included

#include "fennel/common/TraceSource.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/btree/BTreeReader.h"
#include <stdarg.h>

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Class used to dump the contents of a LbmEntry
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LbmEntryDump : public TraceSource
{
    static const uint lineLen = 80;

    /**
     * The level at which tracing of cluster dump will be done.
     * I.e., the caller of this object can control the level at
     * which dumps are generated.
     */
    TraceLevel traceLevel;

public:
    explicit LbmEntryDump(TraceLevel traceLevelInit,
        SharedTraceTarget pTraceTarget, string name);
    uint dump(BTreeDescriptor const &treeDescriptor, bool printRID = false);
};

FENNEL_END_NAMESPACE

#endif

// End LbmEntryDump.h
