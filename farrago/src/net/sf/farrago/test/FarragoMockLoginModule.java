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

package net.sf.farrago.test;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import javax.security.auth.callback.*;

import net.sf.farrago.db.FarragoNoninteractiveCallbackHandler;

import java.util.Vector;
import java.util.Iterator;

/**
 * Mock login module for testing farrago authentication.
 * @author Oscar Gothberg
 * @version $Id$
 */

public class FarragoMockLoginModule implements LoginModule
{
    // REVIEW jvs 19-Mar-2007: alignment spacing below will get
    // removed automatically by Jalopy
    
    CallbackHandler callbackHandler;
    Subject  subject;
    Map      sharedState;
    Map      options;
    
    // REVIEW jvs 19-Mar-2007: use ArrayList instead of Vector unless you
    // actually need fine-grained synchronization.  Which raises the question,
    // what are the thread-safety constraints for this class?

    // Also, the preferred pattern is to declare the variable using
    // an interface type, e.g. List, and then only reference the specific
    // implementation (ArrayList, Vector, ...) at the point of
    // instantiation.  The only exception should be where you need
    // to refer to subclass-specific methods for some reason.
    
    Vector<FarragoMockCredential> tempCredentials;
    Vector<FarragoMockPrincipal> tempPrincipals;
    
    // authentication status
    boolean success;
    
    // config options
    boolean debug;

    // REVIEW jvs 19-Mar-2007:  for all methods (including constructors)
    // Jalopy will move  opening left-curly onto new line
    
    public FarragoMockLoginModule() {
        success = false;
        debug = false;
        tempCredentials = new Vector<FarragoMockCredential>();
        tempPrincipals = new Vector<FarragoMockPrincipal>();
    }
    
    /**
     * Called if LoginContext's required authentications failed.
     */
    public boolean abort()
        throws LoginException
    {
        success = false;
        tempPrincipals.clear();
        tempCredentials.clear();
        
        // REVIEW jvs 19-Mar-2007: is there a way to avoid this?  The
        // login module should be decoupled from the callback handler.
        
        if (callbackHandler instanceof FarragoNoninteractiveCallbackHandler) {
            ((FarragoNoninteractiveCallbackHandler)callbackHandler).clearPassword();
        }
        return true;
    }
    
    /**
     * Called if the LoginContext's required authentications succeeded.
     */
    public boolean commit()
        throws LoginException
    {
        if (success) {
            try {
                subject.getPrincipals().addAll(tempPrincipals);
                subject.getPublicCredentials().addAll(tempCredentials);
                
                tempPrincipals.clear();
                tempCredentials.clear();
            } catch (Exception ex) {
                // REVIEW jvs 19-Mar-2007: Farrago code should not
                // write directly to System.out/err.  To avoid losing
                // the stack, would it work to do
                // loginException.initCause(ex) before throwing it?
                ex.printStackTrace(System.out);
                throw new LoginException(ex.getMessage());
            }
        } else {
            tempPrincipals.clear();
            tempCredentials.clear();
            return false;
        }
        
        return true;
    }

    /**
     * Initialize this LoginModule
     */
    public void initialize(
        Subject subject, 
        CallbackHandler callbackHandler, 
        Map<String, ?> sharedState, 
        Map<String, ?> options)
    {
        // save the initial state
        this.callbackHandler = callbackHandler;
        this.subject     = subject;
        this.sharedState = sharedState;
        this.options     = options;
        
        // REVIEW jvs 19-Mar-2007: Jalopy will insert curly braces
        // automatically for blocks such as the assignment to debug
        // below.
        
        // initialize any configured options
        if (options.containsKey("debug"))
            debug = "true".equalsIgnoreCase((String)options.get("debug"));
    }
    
    /**
     * Try to log in a user.
     */
    public boolean login()
        throws LoginException
    { 
        if (callbackHandler == null) {
            throw new LoginException("No callback handler available");
        }
        
        try {
            Callback[] callbacks = new Callback[] {
                new NameCallback("Username: "),
                new PasswordCallback("Password: ", false)
            };
            
            callbackHandler.handle(callbacks);
            String username = ((NameCallback)callbacks[0]).getName();
            String password = new String(((PasswordCallback)callbacks[1]).getPassword());
            ((PasswordCallback)callbacks[1]).clearPassword();
            
            // hardcoded accts
            if (username.equals("MockLoginModuleTestUser")) {
                // acct testuser requires a correct password
                success = password.equals("secret");
            } else {
                // all other usernames are just let through
                success = true;
            }
            
            if (success) {
                // dummy credential handling that does nothing
                FarragoMockCredential c = new FarragoMockCredential();
                c.setProperty("delete_perm", "0");
                c.setProperty("update_perm", "1");
                this.tempCredentials.add(c);
                this.tempPrincipals.add(new FarragoMockPrincipal(username));
            }

        } catch (Exception ex) {
            success = false;
            throw new LoginException(ex.getMessage());
        }
        
        return true;
    }
    
    /**
     * Log out currently logged in subject
     */
    
    public boolean logout()
        throws LoginException
    {
        tempPrincipals.clear();
        tempCredentials.clear();
        
        if (callbackHandler instanceof FarragoNoninteractiveCallbackHandler)
            ((FarragoNoninteractiveCallbackHandler)callbackHandler).clearPassword();
        
        // remove principals
        Iterator it = subject.getPrincipals(FarragoMockPrincipal.class).iterator();
        while (it.hasNext()) {
            FarragoMockPrincipal p = (FarragoMockPrincipal)it.next();
            subject.getPrincipals().remove(p);
        }
        
        // remove credentials
        it = subject.getPublicCredentials(FarragoMockCredential.class).iterator();
        while (it.hasNext()) {
            FarragoMockCredential c = (FarragoMockCredential)it.next();
            subject.getPublicCredentials().remove(c);
        }
        
        return true;
    }
    
}
