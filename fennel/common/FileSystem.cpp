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

// FIXME:  port
#define __EXTENSIONS__

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/FileSystem.h"

#ifdef __MSVC__
#include <io.h>
#define S_IRUSR S_IREAD
#define S_IWUSR S_IWRITE
typedef int mode_t;
#else
#include <unistd.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#ifdef __MSVC__
#include "fennel/common/FennelResource.h"
#else
#include <sys/statvfs.h>
#endif
#include <fcntl.h>
#include "fennel/common/SysCallExcn.h"
#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

void FileSystem::remove(char const *fileName)
{
    if (doesFileExist(fileName)) {
        setFileAttributes(fileName, 0);
        if (::unlink(fileName)) {
            std::ostringstream oss;
            oss << "Failed to remove file " << fileName;
            throw SysCallExcn(oss.str());
        }
    }
}

bool FileSystem::doesFileExist(char const *filename)
{
    return !::access(filename, 0);
}

bool FileSystem::setFileAttributes(char const *filename,bool readOnly)
{
    mode_t mode = S_IRUSR;
    if (!readOnly) {
        mode |= S_IWUSR;
    }
    return ::chmod(filename, mode) ? 0 : 1;
}

void FileSystem::getDiskFreeSpace(char const *path, FileSize &availableSpace)
{
#ifdef __MSVC__
    throw FennelExcn(
        FennelResource::instance().unsupportedOperation("statvfs"));
#else
    struct statvfs buf;
    int rc = statvfs(path, &buf);
    if (rc == -1) {
        throw SysCallExcn("statvfs call failed");
    }
    availableSpace = buf.f_bsize * buf.f_bavail;
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End FileSystem.cpp
