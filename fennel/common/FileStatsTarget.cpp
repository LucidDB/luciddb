/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/FileStatsTarget.h"
#include "fennel/common/StatsSource.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FileStatsTarget::FileStatsTarget(std::string filenameInit)
{
    filename = filenameInit;
}

std::string FileStatsTarget::getFilename() const
{
    return filename;
}

void FileStatsTarget::beginSnapshot()
{
    assert(!filename.empty());
    snapshotStream.open(filename.c_str(),std::ios::trunc);

    // TODO:  re-enable this.  I disabled it since /tmp/fennel.stats
    // can't be opened on mingw; need to parameterize it better
    // (or put in Performance Monitor integration)
    
    // assert(snapshotStream.good());
}

void FileStatsTarget::endSnapshot()
{
    snapshotStream.close();
}

void FileStatsTarget::writeCounter(std::string name,uint value)
{
    snapshotStream << name << ' ' << value << std::endl;
}

StatsSource::~StatsSource()
{
}

StatsTarget::~StatsTarget()
{
}

FENNEL_END_CPPFILE("$Id$");

// End FileStatsTarget.cpp
