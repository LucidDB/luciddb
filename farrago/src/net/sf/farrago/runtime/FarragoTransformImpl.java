/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import org.eigenbase.util.Util;

/**
 * Default implementation of {@link FarragoTransform}.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public abstract class FarragoTransformImpl implements FarragoTransform
{
    public void init(FarragoRuntimeContext connection, Binding[] bindings)
    {
        // Bind all of the ports.
        //
        // FIXME: Sort bindings into same order as ports first.
        for (int i = 0; i < bindings.length; i++) {
            Binding binding = bindings[i];
            bindPort(binding);
        }
    }

    protected void bindPort(Binding binding)
    {
        final Port port = binding.getPort(this);
        final Object bind = binding.getObjectToBind(this);
        final Object bound = port.bind(bind);
        Util.discard(bound);
    }
}

// End FarragoTransformImpl.java
