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
package net.sf.farrago.parser;

import java.io.*;

import net.sf.farrago.parser.impl.*;


/**
 * FarragoParser is the public wrapper for the JavaCC-generated
 * FarragoParserImpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoParser
    extends FarragoAbstractParser
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new parser.
     */
    public FarragoParser()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAbstractParser
    protected FarragoAbstractParserImpl newParserImpl(Reader reader)
    {
        return new FarragoParserImpl(reader);
    }
}

// End FarragoParser.java
