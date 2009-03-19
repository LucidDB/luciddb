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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/file.h>

void usage();

/**
 * Utility program that preallocates and appends a specified number of pages
 * to the end of a data file.
 *
 * <p>Usage:
 * <br>
 * allocFile --append-pages=&lt;number of pages&gt; --pagesize=&lt;pageSize&gt; &lt;filename&gt;
 *
 * <p>
 * The file must be writable and exclusively lockable.
 */
int
main(int argc, char *argv[])
{
    if (argc != 4) {
        printf("Invalid number of arguments\n");
        usage();
        exit(EINVAL);
    }

    char *fileName = NULL;
    int nPages = 0;
    int pageSize = 0;
    for (uint argIdx = 1; argIdx < argc; argIdx++) {
        if (strncmp(argv[argIdx], "--", 2) != 0) {
            fileName = argv[argIdx];
        } else {
            if (strncmp(&(argv[argIdx][2]), "append-pages=", 13) == 0) {
                nPages = atoi(&(argv[argIdx][15]));
            } else if (strncmp(&(argv[argIdx][2]), "pagesize=", 9) == 0) {
                pageSize = atoi(&(argv[argIdx][11]));
            } else {
                printf("Invalid argument %s\n", argv[argIdx]);
                usage();
                exit(EINVAL);
            }
        }
    }
    if (fileName == NULL) {
        printf("Filename argument not specified\n");
        usage();
        exit(EINVAL);
    }
    if (nPages <= 0) {
        printf("Invalid number of pages argument: %d\n", nPages);
        exit(EINVAL);
    }
    if (pageSize <= 0) {
        printf("Invalid pagesize argument: %d\n", pageSize);
        exit(EINVAL);
    }

    int access = O_LARGEFILE;
    int permission = S_IRUSR;
    access |= O_RDWR;
    permission |= S_IWUSR;

    int handle = open(fileName, access, permission);
    if (handle < 0) {
        printf("Failed to open file '%s'\n", fileName);
        exit(errno);
    }
    if (flock(handle, LOCK_EX | LOCK_NB) < 0) {
        printf("Failed to acquire exclusive lock on file '%s'\n", fileName);
        close(handle);
        exit(errno);
    }
    off_t offset = lseek(handle, 0, SEEK_END);
    if (offset == -1) {
        printf("File seek on file '%s' failed\n", fileName);
        close(handle);
        exit(errno);
    }
    int rc = posix_fallocate(handle, offset, ((off_t) nPages * pageSize));
    if (rc != 0) {
        printf("File allocation failed for file '%s'\n", fileName);
        close(handle);
        exit(rc);
    }
    close(handle);
    exit(0);
}

void usage()
{
    printf("Usage:  allocFile --append-pages=<number of pages> ");
    printf("--pagesize=<pageSize> <filename>\n");
}

// End AllocFile.cpp
