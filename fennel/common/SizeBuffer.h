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

#ifndef Fennel_SharedBuffer_Included
#define Fennel_SharedBuffer_Included

#include <boost/scoped_array.hpp>
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Contains a buffer, its maximum length and current length.
 *
 * @author John Kalucki
 * @since Aug 01, 2005
 * @version $Id$
 **/
class FENNEL_COMMON_EXPORT SizeBuffer : public boost::noncopyable
{
public:
    explicit SizeBuffer(uint capacity, uint length = 0);
    ~SizeBuffer()
    {
    }
    void length(uint length);
    uint length() const;
    uint capacity() const;

    // Renaming buffer() as get() to be in-line with boost auto pointers
    // is problematic. Too easy for a shared pointer to do shared.get()
    // instead of shared->get() and get a pointer to the wrong object.
    // The use of buffer() forces this mistake to be discovered at
    // compilation time
    PBuffer buffer() const;

protected:
    boost::scoped_array<fennel::FixedBuffer> buf;
    uint cap;
    uint len;
};

FENNEL_END_NAMESPACE

#endif

// End SizeBuffer.h
