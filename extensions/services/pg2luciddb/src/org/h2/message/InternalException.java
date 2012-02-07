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
package org.h2.message;

/**
 * This exception wraps a checked exception.
 * It is used in methods where checked exceptions are not supported,
 * for example in a Comparator.
 */
public class InternalException extends RuntimeException 
{

    private static final long serialVersionUID = -5369631382082604330L;
    private Exception cause;

    public InternalException(Exception e) {
        super(e.getMessage());
        cause = e;
    }

    public Exception getOriginalCause() {
        return cause;
    }
}
