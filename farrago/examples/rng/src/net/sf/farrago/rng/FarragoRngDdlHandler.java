/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.rng;

import net.sf.farrago.ddl.*;
import net.sf.farrago.session.*;
import net.sf.farrago.rngmodel.rngschema.*;

import java.io.*;
import java.util.*;

import org.eigenbase.util.*;

/**
 * FarragoRngDdlHandler defines handler methods for random number generator DDL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngDdlHandler extends DdlHandler
{
    FarragoRngDdlHandler(FarragoSessionDdlValidator validator)
    {
        super(validator);
    }

    public void validateDefinition(RngRandomNumberGenerator rng)
    {
        File file = new File(rng.getSerializedFile());
        rng.setSerializedFile(file.getAbsolutePath());
    }

    public void executeCreation(RngRandomNumberGenerator rng)
    {
        try {
            FarragoRngUDR.writeSerialized(rng, new Random());
        } catch (Throwable ex) {
            // TODO:  resource example
            throw Util.newInternal(ex);
        }
    }

    public void executeDrop(RngRandomNumberGenerator rng)
    {
        File file = new File(rng.getSerializedFile());
        file.delete();
    }
}

// End FarragoRngDdlHandler.java
