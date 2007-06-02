/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

// FIXME:  port
#define __EXTENSIONS__

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/FileSystem.h"

// TODO:  trim this
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
//#include <dirent.h>
//#include <fnmatch.h>
#include "fennel/common/SysCallExcn.h"
//#include <errno.h>
#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

void FileSystem::remove(char const *fileName)
{
    if (doesFileExist(fileName)) {
        setFileAttributes(fileName,0);
        if (::unlink(fileName)) {
            std::ostringstream oss;
            oss << "Failed to remove file " << fileName;
            throw SysCallExcn(oss.str());
        }
    }
}

bool FileSystem::doesFileExist(char const *filename)
{
    return !::access(filename,0);
}

bool FileSystem::setFileAttributes(char const *filename,bool readOnly)
{
    mode_t mode = S_IRUSR;
    if (!readOnly) {
        mode |= S_IWUSR;
    }
    return ::chmod(filename,mode) ? 0 : 1;
}

FENNEL_END_CPPFILE("$Id$");

// End FileSystem.cpp
