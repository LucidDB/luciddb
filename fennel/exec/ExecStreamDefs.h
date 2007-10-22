/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_ExecStreamDefs_Included
#define Fennel_ExecStreamDefs_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Identifier for an ExecStream within an instance of
 * ExecStreamGraph.
 */
typedef uint ExecStreamId;

enum ExecStreamBufState 
{
    EXECBUF_EMPTY,
    EXECBUF_NONEMPTY,
    EXECBUF_UNDERFLOW,
    EXECBUF_OVERFLOW,
    EXECBUF_EOS
};

static std::string ExecStreamBufState_names[] = {
    "EXECBUF_EMPTY",
    "EXECBUF_NONEMPTY",
    "EXECBUF_UNDERFLOW",
    "EXECBUF_OVERFLOW",
    "EXECBUF_EOS"
};

static std::string ExecStreamBufState_names_short[] = {
    "EMP",
    "NEM",
    "UND",
    "OVR",
    "EOS"
};

inline std::ostream & operator<< (std::ostream &os, ExecStreamBufState e)
{
    return os << ExecStreamBufState_names[e];
}

enum ExecStreamBufProvision
{
    BUFPROV_NONE,
    BUFPROV_CONSUMER,
    BUFPROV_PRODUCER,
};

enum ExecStreamResult
{
    EXECRC_BUF_UNDERFLOW,
    EXECRC_BUF_OVERFLOW,
    EXECRC_EOS,
    EXECRC_QUANTUM_EXPIRED,
    EXECRC_YIELD
};

static std::string ExecStreamResult_names[] = {
    "EXECRC_BUF_UNDERFLOW",
    "EXECRC_BUF_OVERFLOW",
    "EXECRC_EOS",
    "EXECRC_QUANTUM_EXPIRED",
    "EXECRC_YIELD"
};

static std::string ExecStreamResult_names_short[] = {
    "UND",
    "OVR",
    "EOS",
    "QNT",
    "YLD"
};

inline std::ostream & operator<< (std::ostream &os, ExecStreamResult e)
{
    return os << ExecStreamResult_names[e];
}

/**
 * ExecStreamQuantum defines the quantum for scheduling of an ExecStream.  The
 * exact interpretation of the specified quantities is stream-dependent.  For
 * example, for nTuplesMax, a filter might count number of input tuples, while
 * a join might count number of tuple comparisons.
 */
struct ExecStreamQuantum
{
    /**
     * Maximum number of tuples to process per quantum.
     */
    uint nTuplesMax;

    /**
     * Creates a new quantum, initially unlimited.
     */
    explicit ExecStreamQuantum()
    {
        nTuplesMax = MAXU;
    }
};
    
/**
 * Enumerated type that indicates the nature of a resource requirement setting
 * for an execution stream
 */
enum ExecStreamResourceSettingType {
    /**
     * Setting is accurate
     */
    EXEC_RESOURCE_ACCURATE,
    /**
     * Setting is an estimate, which may or may not be based on up-to-date
     * statistics.  Therefore, if possible, additional resources may be
     * granted to a stream using this setting.
     */
    EXEC_RESOURCE_ESTIMATE,
    /**
     * Setting is unknown and the stream requires maximum resources
     */
    EXEC_RESOURCE_UNBOUNDED
};

/**
 * ExecStreamResourceQuantity quantifies various resources which
 * can be allocated to an ExecStream.
 */
struct ExecStreamResourceQuantity
{
    /**
     * Number of dedicated threads the stream may request while executing.
     * Non-parallelized streams have 0 for this setting, meaning the only
     * threads which execute them are managed by the scheduler instead.
     */
    uint nThreads;

    /**
     * Number of cache pages the stream may pin while executing.  This includes
     * both scratch pages and I/O pages used for storage access.
     */
    uint nCachePages;

    explicit ExecStreamResourceQuantity()
    {
        nThreads = 0;
        nCachePages = 0;
    }
};

/**
 * Common parameters for instantiating any ExecStream.
 */
struct ExecStreamParams 
{
    /**
     * CacheAccessor to use for any data access.  This will be singular if the
     * stream should not perform data access.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Accessor for segment to use for allocating scratch buffers.  This will
     * be singular if the stream should not use any scratch buffers.
     */
    SegmentAccessor scratchAccessor;

    explicit ExecStreamParams();

    virtual ~ExecStreamParams();
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamDefs.h
