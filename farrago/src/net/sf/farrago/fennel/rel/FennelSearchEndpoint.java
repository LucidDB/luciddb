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
package net.sf.farrago.fennel.rel;

import java.math.*;


/**
 * FennelSearchEndpoint defines an enumeration corresponding to
 * fennel/common/SearchEndpoint.h. Any changes there must be applied here as
 * well.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum FennelSearchEndpoint
{
    /**
     * Defines the beginning of an interval which is unbounded below. The
     * associated key value should be all null.
     */
    SEARCH_UNBOUNDED_LOWER('-'),

    /**
     * Defines the beginning of an interval which has an open bound below.
     */
    SEARCH_OPEN_LOWER('('),

    /**
     * Defines the beginning of an interval which has a closed bound below.
     */
    SEARCH_CLOSED_LOWER('['),

    /**
     * Defines the end of an interval which has an open bound above.
     */
    SEARCH_OPEN_UPPER(')'),

    /**
     * Defines the end of an interval which has a closed bound above.
     */
    SEARCH_CLOSED_UPPER(']'),

    /**
     * Defines the end of an interval which is unbounded above. The associated
     * key value should be all null.
     */
    SEARCH_UNBOUNDED_UPPER('+');

    private final String symbol;

    private FennelSearchEndpoint(char symbol)
    {
        this.symbol = new String(new char[] { symbol });
    }

    /**
     * @return symbol used to communicate endpoint type to Fennel
     */
    public String getSymbol()
    {
        return symbol;
    }
}

// End FennelSearchEndpoint.java
