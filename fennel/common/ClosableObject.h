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

#ifndef Fennel_ClosableObject_Included
#define Fennel_ClosableObject_Included

FENNEL_BEGIN_NAMESPACE

/**
 * ClosableObject is a common base for all classes which require a close()
 * method to be called before destruction (e.g. to avoid the error of calling
 * virtual methods from a base class destructor).  Deriving from ClosableObject
 * makes it easier to instantiate a shared_ptr which takes care of automatic
 * close on destruction (see ClosableObjectDestructor).
 */
class FENNEL_COMMON_EXPORT ClosableObject
{
protected:
    /**
     * Must be implemented by derived class to release any resources.
     */
    virtual void closeImpl() = 0;

    bool needsClose;

    explicit ClosableObject();

public:
    /**
     * Destructor.  An assertion violation will result if the object has not
     * yet been closed.
     */
    virtual ~ClosableObject();

    /**
     * @return whether the object has been closed
     */
    bool isClosed() const
    {
        return !needsClose;
    }

    /**
     * Closes this object, releasing any unallocated resources.
     */
    void close();
};

/**
 * A destructor functor for use as the "D" parameter to a
 * boost::shared_ptr constructor.
 */
class FENNEL_COMMON_EXPORT ClosableObjectDestructor
{
public:
    void operator()(ClosableObject *pClosableObject)
    {
        pClosableObject->close();
        delete pClosableObject;
    }
};

FENNEL_END_NAMESPACE

#endif

// End ClosableObject.h
