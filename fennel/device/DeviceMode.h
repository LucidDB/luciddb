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

#ifndef Fennel_DeviceMode_Included
#define Fennel_DeviceMode_Included

FENNEL_BEGIN_NAMESPACE

struct FENNEL_DEVICE_EXPORT DeviceMode
{
    bool create : 1;
    bool readOnly : 1;
    bool temporary : 1;
    bool direct : 1;
    bool sequential : 1;

    enum Initializer { load = 0, createNew = 1 };

    explicit DeviceMode()
    {
        init();
    }

    DeviceMode(Initializer i)
    {
        init();
        create = i;
    }

    DeviceMode(DeviceMode const &mode)
    {
        init(mode);
    }

    void operator = (DeviceMode const &mode)
    {
        init(mode);
    }

private:
    void init()
    {
        create = 0;
        readOnly = 0;
        temporary = 0;
        direct = 0;
        sequential = 0;
    }

    void init(DeviceMode const &mode)
    {
        create = mode.create;
        readOnly = mode.readOnly;
        temporary = mode.temporary;
        direct = mode.direct;
        sequential = mode.sequential;
    }
};

FENNEL_END_NAMESPACE

#endif

// End DeviceMode.h
