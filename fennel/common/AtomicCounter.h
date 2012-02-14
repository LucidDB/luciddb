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

#ifndef Fennel_AtomicCounter_Included
#define Fennel_AtomicCounter_Included

FENNEL_BEGIN_NAMESPACE

/**
 * AtomicCounter wraps STL support for atomic increment/decrement.
 */
class FENNEL_COMMON_EXPORT AtomicCounter : public _STL::_Refcount_Base
{
public:
    explicit AtomicCounter() : _Refcount_Base(0)
    {
    }

    operator uint() const
    {
        return _M_ref_count;
    }

    void clear()
    {
        _M_ref_count = 0;
    }

    void operator ++ ()
    {
        _M_incr();
    }

    void operator -- ()
    {
        _M_decr();
    }
};

FENNEL_END_NAMESPACE

#endif

// End AtomicCounter.h
