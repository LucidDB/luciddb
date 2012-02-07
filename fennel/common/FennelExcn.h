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

#ifndef Fennel_FennelExcn_Included
#define Fennel_FennelExcn_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Base class for all Fennel exceptions.
 */
class FENNEL_COMMON_EXPORT FennelExcn : public std::exception
{
protected:
    std::string msg;

public:
    /**
     * Construct a new FennelExcn.
     *
     * @param msgInit message
     */
    explicit FennelExcn(std::string msgInit);

    virtual ~FennelExcn() throw();

    // implement std::exception
    virtual const char *what() const throw();

    virtual void throwSelf();

    std::string const &getMessage()
    {
        return msg;
    }
};

FENNEL_END_NAMESPACE

#endif

// End FennelExcn.h
