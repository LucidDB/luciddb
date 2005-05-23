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

#ifndef Fennel_FileSystem_Included
#define Fennel_FileSystem_Included

FENNEL_BEGIN_NAMESPACE

/**
 * FileSystem provides some static utility methods for manipulating the OS
 * file system.
 */
class FileSystem
{
public:
    static void remove(char const *filename);
    static bool setFileAttributes(char const *filename,bool readOnly = 1);
    static bool doesFileExist(char const *filename);
    static bool getDiskFreeSpace(char const *dir, FileSize *availableSpace);
};

FENNEL_END_NAMESPACE

#endif

// End FileSystem.h
