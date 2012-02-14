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
package org.eigenbase.applib.resource;

// NOTE:  This class gets compiled independently of everything else so that
// resource generation can use reflection.  That means it must have no
// dependencies on other Eigenbase code.
import java.util.logging.*;

import org.eigenbase.util.*;


/**
 * Exception class for Applib
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ApplibException
    extends EigenbaseException
{
    //~ Static fields/initializers ---------------------------------------------

    private static Logger tracer =
        Logger.getLogger(ApplibException.class.getName());

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new ApplibException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public ApplibException(String message, Throwable cause)
    {
        super(message, cause);

        // TODO: Force the caller to pass in a Logger as a trace argument for
        // better context.  Need to extend ResGen for this.
        //tracer.throwing("ApplibException", "constructor", this);
        //tracer.severe(toString());
    }
}

// End ApplibException.java
