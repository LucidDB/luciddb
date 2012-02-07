/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
 * <p>Usage:<br>
 *
 * allocFile --append-pages=&lt;number of pages&gt;
 * --pagesize=&lt;pageSize&gt; &lt;filename&gt;
 *
 * <p>The file must be writable and exclusively lockable.
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
