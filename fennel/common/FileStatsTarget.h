/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_FileStatsTarget_Included
#define Fennel_FileStatsTarget_Included

#include "fennel/common/StatsTarget.h"

#include <fstream>

FENNEL_BEGIN_NAMESPACE

/**
 * FileStatsTarget implements the StatsTarget interface by writing to a simple
 * text file.
 */
class FENNEL_COMMON_EXPORT FileStatsTarget : public StatsTarget
{
    std::string filename;
    std::ofstream snapshotStream;

public:
    /**
     * Creates a new FileStatsTarget.
     *
     * @param filename name of file into which to write stats
     */
    explicit FileStatsTarget(std::string filename);

    /**
     * Gets name of file receiving stats.
     */
    std::string getFilename() const;

    // implement the StatsTarget interface
    virtual void beginSnapshot();
    virtual void endSnapshot();
    virtual void writeCounter(std::string name,int64_t value);
};

FENNEL_END_NAMESPACE

#endif

// End FileStatsTarget.h
