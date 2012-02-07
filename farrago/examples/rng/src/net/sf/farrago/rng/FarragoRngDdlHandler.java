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
            FarragoRngUDR.writeSerialized(
                new File(FarragoRngUDR.getFilename(rng)),
                random);
        } catch (Throwable ex) {
            throw FarragoRngPluginFactory.res.RngFileCreationFailed.ex(
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
