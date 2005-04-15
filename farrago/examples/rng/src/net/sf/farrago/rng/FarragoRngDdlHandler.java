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
import net.sf.farrago.util.*;
import net.sf.farrago.rngmodel.rngschema.*;
import net.sf.farrago.rng.resource.*;

import java.net.*;
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
    FarragoRngDdlHandler(
        FarragoSessionDdlValidator validator)
    {
        super(validator);
    }

    public void validateDefinition(RngRandomNumberGenerator rng)
    {
        String path = rng.getSerializedFile();
        String expandedPath =
            FarragoProperties.instance().expandProperties(path);
        if (path.equals(expandedPath)) {
            rng.setSerializedFile(new File(path).getAbsolutePath());
        }
    }

    public void executeCreation(RngRandomNumberGenerator rng)
    {
        Random random;
        Long seed = rng.getInitialSeed();
        if (seed == null) {
            random = new Random();
        } else {
            random = new Random(seed.longValue());
        }
        try {
            FarragoRngUDR.writeSerialized(rng, random);
        } catch (Throwable ex) {
            throw FarragoRngPluginFactory.res.newRngFileCreationFailed(
                validator.getRepos().getLocalizedObjectName(
                    rng.getSerializedFile()),
                validator.getRepos().getLocalizedObjectName(rng),
                ex);
        }
    }

    public void executeDrop(RngRandomNumberGenerator rng)
    {
        File file = new File(rng.getSerializedFile());
        file.delete();
    }
}

// End FarragoRngDdlHandler.java
