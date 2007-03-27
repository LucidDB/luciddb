/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

package net.sf.farrago.db;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.callback.*;

/**
 * Callback handler for situations where user/pass is already retrieved.
 * @author ogothberg
 * @version $Id$
 */

public class FarragoNoninteractiveCallbackHandler
    implements CallbackHandler
{
    String user, pass;
    
    public FarragoNoninteractiveCallbackHandler(String user, String pass) {
        this.user = user;
        this.pass = (pass == null? "":pass);
    }

    public void clearPassword() {
        pass = "";
    }
    
    /**
     * Just pass through user/pass to the corresponding callbacks
     */
    public void handle(Callback[] callbacks)
        throws IOException, UnsupportedCallbackException
    {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
                ((NameCallback)callbacks[i]).setName(user);
            } else if (callbacks[i] instanceof PasswordCallback) {
                ((PasswordCallback)callbacks[i]).setPassword(pass.toCharArray());
            } else {
                throw (new UnsupportedCallbackException(
                    callbacks[i], "Unsupported callback class"));
            }
        }
    }

}
