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
package net.sf.farrago.type.runtime;

import java.io.*;

import org.eigenbase.util.*;


/**
 * EncodedCharPointer specializes BytePointer to interpret its bytes as
 * characters encoded via a given charset.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class EncodedCharPointer
    extends BytePointer
{
    //~ Methods ----------------------------------------------------------------

    // TODO:  preallocate a CharsetDecoder
    public String toString()
    {
        if (buf == null) {
            return null;
        }
        try {
            return new String(
                buf,
                pos,
                count - pos,
                getCharsetName());
        } catch (UnsupportedEncodingException ex) {
            throw Util.newInternal(ex);
        }
    }

    // refine BytePointer to make this method abstract so that subclasses
    // are forced to override it
    protected abstract String getCharsetName();

    // implement BytePointer
    protected byte [] getBytesForString(String string)
    {
        try {
            return string.getBytes(getCharsetName());
        } catch (UnsupportedEncodingException ex) {
            throw Util.newInternal(ex);
        }
    }

    // override BytePointer
    public Object getNullableData()
    {
        return toString();
    }
}

// End EncodedCharPointer.java
