/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.ddl;

import net.sf.farrago.fem.security.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;
import java.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import net.sf.farrago.resource.*;


    
/**
 * DdlGrantStmt represents a DDL GRANT statement.
 * 
 *
 * @author Quoc Tai Tran
 * @version $Id$
 */
public class DdlGrantStmt extends DdlStmt
{
    private CwmModelElement grantedObject;
    private List privList;
    private SqlIdentifier grantor;
    private boolean grantOption;
    private boolean hierarchyOption;
    boolean currentRoleOption;
    boolean currentUserOption;
    private List granteeList;    
    private MultiMap privilegeMap; //TODO: to be moved to session level
    

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructs a new DdlGrantStmt.
     *
     */
    public DdlGrantStmt()
    {
        super(null);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    // TODO: Modeling of grant dependencies,  so that we can revoke cascade.
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoRepos repos =  ddlValidator.getRepos();

        FemAuthId grantorAuthId;
        if (grantor == null || currentUserOption == true) {
            // The grantor is not specified, then use the current
            // session' user named as our grantor name
            String grantorName = ddlValidator.getInvokingSession().
                getSessionVariables().sessionUserName;
            grantorAuthId = findAuthIdByName(repos, grantorName);
        }
        // TODO: retrieve the current role from the session and set that to
        // be the grantor
        // else if (currentRoleOption == true) {
        // }
        else {
            // TODO: grantor is specified, then check that grantor can be
            // impersonated by the session ID. Check standard to be sure!
            grantorAuthId = findAuthIdByName(repos, grantor.getSimple());
        }
        
        // TODO: ensure grantor is the owner or someone with the GRANT OPTION
        // privilege

        // initialized the privilege lookup table. 
        privilegeMap = initGrantValidationLookupMap(repos);
        
        Iterator iter = granteeList.iterator();
        while(iter.hasNext()) {

            // process the next grantee
            SqlIdentifier id = (SqlIdentifier) iter.next();

            // Find the repository element id for the grantee,  create one if
            // it does not exist
            FemAuthId granteeAuthId = findAuthIdByName(repos,
                id.getSimple());

            // for each privilege in the list,  we instantiate a repository
            // element. Note that this makes it easier to revoke the privs on
            // the individual basis.
            Iterator iterPriv = privList.iterator();
            while (iterPriv.hasNext()) {
                SqlIdentifier privId = (SqlIdentifier) iterPriv.next();

                // make sure that the privilege is appropriate for the object
                // type. 
                List legalList = privilegeMap.getMulti(grantedObject.refClass());

                if (!legalList.contains(privId.getSimple().toUpperCase()))
                {
                    // throw an exception, we see an illegal privilege
                    throw FarragoResource.instance().newValidatorInvalidGrant(
                        privId.getSimple(),grantedObject.getName());
                }
                
                // TODO: to check that the grantor must be the owner. Need model change.
                
                
                // TODO: to check that if not an owner, then the grantor must
                // have the GRANT option on the privilege specified.
                
                // create a privilege object and set its properties
                FemGrant grant = repos.newFemGrant();
                
                // set the privilege name (i.e. action) and properties
                grant.setAction(privId.getSimple());
                grant.setWithGrantOption(grantOption);

                // TODO: to grant.setHierarchyOption(hierarchyOption);

                // associate the privilege with the 
                grant.setGrantor(grantorAuthId);
                grant.setGrantee(granteeAuthId);
                grant.setElement(grantedObject);
            }
        }
    }

    
    public void setPrivList(List privList)
    {
        this.privList = privList;
    }

    public void setGrantedObject(CwmModelElement grantedObject)
    {
        this.grantedObject = grantedObject;
    }

    public void setGranteeList(List granteeList)
    {
        this.granteeList = granteeList;
    }


    public void setGrantor(SqlIdentifier grantor)
    {
        this.grantor = grantor;
    }

    public void setHierarchyOption(boolean hierarchyOption)
    {
        this.hierarchyOption = hierarchyOption;
    }

    public void setGrantOption(boolean grantOption)
    {
        this.grantOption = grantOption;
    }

    public void setCurrentRoleOption(boolean currentRoleOption)
    {
        this.currentRoleOption = currentRoleOption;
    }

    public void setCurrentUserOption(boolean currentUserOption)
    {
        this.currentUserOption = currentUserOption;
    }

    private FemAuthId findAuthIdByName(
        FarragoRepos repos, String authName)
    {
        Collection authIdCollection =
            repos.getSecurityPackage().getFemAuthId().
            refAllOfType();
        FemAuthId femAuthId =
            (FemAuthId)
            FarragoCatalogUtil.getModelElementByName(
                authIdCollection, authName);

        if (femAuthId == null) {
            // need to create a new auth id instance for metadata repository
            femAuthId = repos.newFemUser();
            femAuthId.setName(authName);
        }
        return femAuthId;
    }

    
    private MultiMap initGrantValidationLookupMap (FarragoRepos repos)
    {
        // TODO: This routine is temporary. We need to have an extensible way
        // of handling new kind of privileges. Plus we must be move to session level so that
        // we don't have to initialize it on the per GRANT request basis.

        // TODO: we want to dynamically load any type of privileges associate
        // with each access controlled object types e.g. TABLE, VIEW,  PROCEDURE etc.

        // Populate the privilege validation table. The key is the object type
        // such as TABLE,  SEQUENCE etc. and the value of the entry will be a
        // list of privileges legal for an object type. 
        String[] tabPrivs = {"SELECT", "INSERT",  "DELETE",  "UPDATE"};
        String[] seqPrivs = {"SELECT"};
        String[] procPrivs = {"EXECUTE"};        

        MultiMap pMap = new MultiMap();

        // Table prvileges
        for (int i = 0; i < tabPrivs.length; i++)
        {   
            pMap.putMulti(repos.getMedPackage().getFemLocalTable(), tabPrivs[i]);
        }

        // Sequence prvileges. TODO
//         for (int i = 0; i < tabPrivs.length; i++)
//         {   
//             pMap.putMulti(FemSequence, tabPrivs[i]);
//         }

        // Procedure prvileges
        for (int i = 0; i < procPrivs.length; i++)
        {   
            pMap.putMulti(repos.getSql2003Package().getFemRoutine(), procPrivs[i]);
        }

        return pMap;
    }
}

// End DdlGrantStmt.java
