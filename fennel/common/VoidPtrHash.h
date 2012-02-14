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

#ifndef Fennel_VoidPtrHash_Included
#define Fennel_VoidPtrHash_Included

#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * VoidPtrHash can be used to create a hash_map or hash_set with pointers as
 * keys.
 */
struct FENNEL_COMMON_EXPORT VoidPtrHash
{
    size_t operator() (void *key) const
    {
        return reinterpret_cast<size_t>(key);
    }
};

template<typename T>
struct FENNEL_COMMON_EXPORT VoidSharedPtrHash
{
    size_t operator() (boost::shared_ptr<T> key) const
    {
        return reinterpret_cast<size_t>(key.get());
    }
};

FENNEL_END_NAMESPACE

#endif

// End VoidPtrHash.h
