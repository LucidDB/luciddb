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

#ifndef Fennel_SimpleExecStreamGovernor_Included
#define Fennel_SimpleExecStreamGovernor_Included

#include "fennel/exec/ExecStreamGovernor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * SimpleExecStreamGovernor is a reference implementation of
 * ExecStreamGovernor.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class SimpleExecStreamGovernor : public ExecStreamGovernor
{
    /**
     * Assigns each stream the specified number of cache pages
     *
     * @param streams streams to be assigned resources
     * @param nCachePages array containing number of cache pages to assign
     * each stream
     */
    void assignCachePages(
        std::vector<SharedExecStream> &streams,
        boost::scoped_array<uint> const &nCachePages);

    /**
     * Distributes cache pages across streams according to the following
     * formula:
     *
     * <pre><code>
     * base amount for stream +
     *  (# of extra pages above total min available to the stream graph) *
     *  sqrt((opt for stream) - (min for stream)) /
     *  sum of the sqrt of the differences between the opt and min for streams
     *      that will receive excess allocations
     *
     *  where the base amount is either the min or opt requirements of the
     *  stream
     * </code></pre>
     *
     * @param streams streams to be assigned resources
     * @param minReqts min cache pages required by each stream
     * @param optReqts opt cache pages required by each stream
     * @param optTypes optimum type setting of each stream
     * @param sqrtDiffOptMin sqrt of the difference between the opt and min
     * for each stream
     * @param totalSqrtDiffs sum of the sqrt of the differences between the
     * opt and min for each stream
     * @param excessAvailable excess cache pages to be distributed across
     * certain streams
     * @param assignOpt if true, assign at least the optimum amount to each
     * stream; otherwise, assign at least the minimum amount
     *
     * @return total number of cache pages assigned to streams
     */
    uint distributeCachePages(
        std::vector<SharedExecStream> &streams,
        boost::scoped_array<uint> const &minReqts,
        boost::scoped_array<uint> const &optReqts,
        boost::scoped_array<ExecStreamResourceSettingType> const &optTypes,
        boost::scoped_array<double> const &sqrtDiffOptMin,
        double totalSqrtDiffs, 
        uint excessAvailable, bool assignOpt);

public:
    explicit SimpleExecStreamGovernor(
        ExecStreamResourceKnobs const &knobSettings,
        ExecStreamResourceQuantity const &resourcesAvailable,
        SharedTraceTarget pTraceTarget,
        std::string name);

    virtual ~SimpleExecStreamGovernor();

    // implement the ExecStreamGovernor interface
    virtual void requestResources(SharedExecStreamGraph pExecStreamGraph);
    virtual void returnResources(ExecStreamGraph *pExecStreamGraph);
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGovernor.h
