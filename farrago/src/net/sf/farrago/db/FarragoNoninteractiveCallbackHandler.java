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
package net.sf.farrago.db;

import java.io.*;

import javax.security.auth.callback.*;


/**
 * Callback handler for situations where user/pass is already retrieved.
 *
 * @author ogothberg
 * @version $Id$
 */

public class FarragoNoninteractiveCallbackHandler
    implements CallbackHandler
{
    //~ Instance fields --------------------------------------------------------

    String user, pass;

    //~ Constructors -----------------------------------------------------------

    public FarragoNoninteractiveCallbackHandler(String user, String pass)
    {
        this.user = user;
        this.pass = ((pass == null) ? "" : pass);
    }

    //~ Methods ----------------------------------------------------------------

    public void clearPassword()
    {
        pass = "";
    }

    /**
     * Just pass through user/pass to the corresponding callbacks
     */
    public void handle(Callback [] callbacks)
        throws IOException, UnsupportedCallbackException
    {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
                ((NameCallback) callbacks[i]).setName(user);
            } else if (callbacks[i] instanceof PasswordCallback) {
                ((PasswordCallback) callbacks[i]).setPassword(
                    pass.toCharArray());
            } else {
                throw new UnsupportedCallbackException(
                    callbacks[i],
                    "Unsupported callback class");
            }
        }

        // user/pass have been passed on to callbacks,
        // don't keep them in the handler
        user = null;
        pass = null;
    }
}

// End FarragoNoninteractiveCallbackHandler.java
