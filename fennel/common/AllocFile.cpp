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

#include "fennel/common/CommonPreamble.h"
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <errno.h>
#include <sys/file.h>
#include <unistd.h>

void usage();

/**
 * Utility program that preallocates and appends a specified number of pages
 * to the end of a data file.
 *
 * <p>Usage:
 * <br>
 * allocFile --append-pages=<number of pages> --pagesize=<pageSize> <filename>
 *
 * <p>
 * The file must be writable and exclusively lockable.
 */
int
main(int argc, char *argv[])
{
    if (argc != 4) {
        std::cerr << "Invalid number of arguments" << std::endl;
        usage();
        exit(EINVAL);
    }

    char *fileName = NULL;
    int64_t nPages = 0;
    int pageSize = 0;
    for (uint argIdx = 1; argIdx < argc; argIdx++) {
        if (strncmp(argv[argIdx], "--", 2) != 0) {
            fileName = argv[argIdx];
        } else {
            if (strncmp(&(argv[argIdx][2]), "append-pages=", 13) == 0) {
                nPages = atoi(&(argv[argIdx][15]));
            }
            else if (strncmp(&(argv[argIdx][2]), "pagesize=", 9) == 0) {
                pageSize = atoi(&(argv[argIdx][11]));
            } else {
                std::cerr << "Invalid argument " << argv[argIdx] << std::endl;
                usage();
                exit(EINVAL);
            }
        }
    }
    if (fileName == NULL) {
        std::cerr << "Filename argument not specified" << std::endl;
        usage();
        exit(EINVAL);
    }
    if (nPages <= 0) {
        std::cerr << "Invalid number of pages argument: " << nPages
            << std::endl;
        usage();
        exit(EINVAL);
    }
    if (pageSize <= 0) {
        std::cerr << "Invalid pagesize argument: " << pageSize << std::endl;
        usage();
        exit(EINVAL);
    }

    int access = O_LARGEFILE;
    int permission = S_IRUSR;
    access |= O_RDWR;
    permission |= S_IWUSR;

    int handle = open(fileName, access, permission);
    if (handle < 0) {
        std::cerr << "Failed to open '" << fileName << "'" << std::endl;
        exit(errno);
    }
    if (flock(handle, LOCK_EX|LOCK_NB) < 0) {
        std::cerr << "Failed to acquire exclusive lock on '" << fileName
            << "'" << std::endl;
        close(handle);
        exit(errno);
    }
    off_t offset = lseek(handle, 0, SEEK_END);
    if (offset == -1) {
        std::cerr << "File seek on '" << fileName << "' failed " << std::endl;
        close(handle);
        exit(errno);
    }
    int rc = posix_fallocate(handle, offset, (off_t) (nPages * pageSize));
    if (rc != 0) {
        std::cerr << "File allocation failed for " << fileName << std::endl;
        close(handle);
        exit(rc);
    }
    close(handle);
    exit(0);
}

void usage()
{
    std::cerr << "Usage:  allocFile --append-pages=<number of pages> " <<
       "--pagesize=<pageSize> <filename>" << std::endl;
}

// End AllocFile.cpp
