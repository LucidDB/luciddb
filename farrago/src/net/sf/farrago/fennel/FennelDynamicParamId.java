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
package net.sf.farrago.fennel;

import net.sf.farrago.fem.fennel.*;


/**
 * FennelDynamicParamId is an opaque type for the 32-bit integers used to
 * uniquely identify dynamic parameters within a {@link FennelStreamGraph}.
 * Fennel dynamic parameters may be used to implement user-level dynamic
 * parameters (represented as question marks in SQL text); they may also be
 * generated internally by the optimizer as part of physical implementation. In
 * the latter case, they are used for out-of-band communication between
 * ExecStreams which cannot be expressed via the usual producer/consumer
 * dataflow mechanisms (e.g. when the streams are not adjacent, or when
 * communication is required from consumer back to producer).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDynamicParamId
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Indicates whether a stream that accesses a dynamic parameter produces or
     * consumes the dynamic parameter
     */
    public enum StreamType
    {
        PRODUCER, CONSUMER, UNKNOWN
    }

    //~ Instance fields --------------------------------------------------------

    private final int id;

    private FemExecutionStreamDef producerStream;

    private FemExecutionStreamDef consumerStream;

    //~ Constructors -----------------------------------------------------------

    public FennelDynamicParamId(int id)
    {
        this(id, null, StreamType.UNKNOWN);
    }

    public FennelDynamicParamId(
        int id,
        FemExecutionStreamDef streamDef,
        StreamType streamType)
    {
        this.id = id;
        if (streamType == StreamType.CONSUMER) {
            consumerStream = streamDef;
            producerStream = null;
        } else if (streamType == StreamType.PRODUCER) {
            producerStream = streamDef;
            consumerStream = null;
        } else {
            producerStream = null;
            consumerStream = null;
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying int value
     */
    public int intValue()
    {
        return id;
    }

    /**
     * Associates a stream with the dynamic parameter. The stream either
     * produces or consumes the dynamic parameter.
     *
     * @param streamDef the stream
     * @param streamType whether the stream produces or consumes the parameter
     */
    public void associateStream(
        FemExecutionStreamDef streamDef,
        StreamType streamType)
    {
        if (streamType == StreamType.PRODUCER) {
            assert (producerStream == null);
            producerStream = streamDef;
        } else if (streamType == StreamType.CONSUMER) {
            assert (consumerStream == null);
            consumerStream = streamDef;
        } else {
            assert ((streamDef == null) && (streamType == StreamType.UNKNOWN));
        }
    }

    /**
     * @return the stream that produces this dynamic parameter
     */
    public FemExecutionStreamDef getProducerStream()
    {
        assert (consumerStream != null);
        return producerStream;
    }

    /**
     * @return the stream that consumes this dynamic parameter
     */
    public FemExecutionStreamDef getConsumerStream()
    {
        assert (producerStream != null);
        return consumerStream;
    }

    // implement Object
    public boolean equals(Object other)
    {
        return ((other instanceof FennelDynamicParamId)
            && (((FennelDynamicParamId) other).intValue() == id));
    }

    // implement Object
    public int hashCode()
    {
        return id;
    }

    // implement Object
    public String toString()
    {
        return Integer.toString(id);
    }
}

// End FennelDynamicParamId.java
