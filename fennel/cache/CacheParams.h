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

#ifndef Fennel_CacheParams_Included
#define Fennel_CacheParams_Included

#include "fennel/device/DeviceAccessSchedulerParams.h"

FENNEL_BEGIN_NAMESPACE

class ConfigMap;

/**
 * CacheParams defines parameters used to instantiate a Cache.
 */
class CacheParams 
{
public:
    static ParamName paramMaxPages;
    static ParamName paramPagesInit;
    static ParamName paramPageSize;
    static ParamName paramIdleFlushInterval;

    /**
     * Parameters for instantiating DeviceAccessScheduler.
     */
    DeviceAccessSchedulerParams schedParams;

    /**
     * Maximum number of buffer pages the cache can
     * manage.
     */
    uint nMemPagesMax;

    /**
     * Number of bytes per page.
     */
    uint cbPage;

    /**
     * Initial number of page buffers to allocate (up to nMemPagesMax).
     */
    uint nMemPagesInit;

    /**
     * Number of milliseconds between idle flushes, or 0 to disable.
     */
    uint idleFlushInterval;

    /**
     * Define a default set of cache parameters.
     */
    explicit CacheParams();

    /**
     * Read parameter settings from a ConfigMap.
     */
    void readConfig(ConfigMap const &configMap);
};

FENNEL_END_NAMESPACE

#endif

// End CacheParams.h
