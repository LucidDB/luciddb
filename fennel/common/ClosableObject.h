/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
