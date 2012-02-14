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
package org.luciddb.lcs;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;


/**
 * LcsCompositeStreamDef is a composite Fem stream definition.
 *
 * <pre>
 * generator -> sorter -> splicer
 * </pre>
 *
 * @author John Pham
 * @version $Id$
 */
class LcsCompositeStreamDef
{
    //~ Instance fields --------------------------------------------------------

    private FemExecutionStreamDef consumer;
    private FemExecutionStreamDef producer;

    //~ Constructors -----------------------------------------------------------

    public LcsCompositeStreamDef(
        FemExecutionStreamDef consumer,
        FemExecutionStreamDef producer)
    {
        this.consumer = consumer;
        this.producer = producer;
    }

    //~ Methods ----------------------------------------------------------------

    public FemExecutionStreamDef getConsumer()
    {
        return consumer;
    }

    public FemExecutionStreamDef getProducer()
    {
        return producer;
    }
}

// End LcsCompositeStreamDef.java
