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

#ifndef Fennel_FileSystem_Included
#define Fennel_FileSystem_Included

FENNEL_BEGIN_NAMESPACE

/**
 * FileSystem provides some static utility methods for manipulating the OS
 * file system.
 */
class FENNEL_COMMON_EXPORT FileSystem
{
public:
    static void remove(char const *filename);
    static bool setFileAttributes(char const *filename,bool readOnly = 1);
    static bool doesFileExist(char const *filename);

    /**
     * Determines how much free space is available in a file system.
     *
     * @param path the pathname of any file within the file system
     * @param availableSpace returns the number of free bytes available in the
     * file system
     */
    static void getDiskFreeSpace(char const *path, FileSize &availableSpace);
};

FENNEL_END_NAMESPACE

#endif

// End FileSystem.h
