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
package net.sf.farrago.namespace.mdr;

import java.sql.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;

import org.eigenbase.jmi.*;
import org.eigenbase.reltype.*;


/**
 * MedMdrNameDirectory implements the FarragoMedNameDirectory interface by
 * mapping the package structure of an MDR repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrNameDirectory
    extends MedAbstractNameDirectory
{
    //~ Instance fields --------------------------------------------------------

    final MedMdrDataServer server;
    final RefPackage rootPackage;

    //~ Constructors -----------------------------------------------------------

    /**
     * Instantiates a MedMdrNameDirectory.
     *
     * @param server MedMdrDataServer from which this directory was opened
     * @param rootPackage root package from which to map names in repository;
     * so, when a table name like x.y.z is looked up, rootPackage will be
     * searched for subpackage x, which will be searched for subpackage y, which
     * will be searched for class z
     */
    MedMdrNameDirectory(
        MedMdrDataServer server,
        RefPackage rootPackage)
    {
        this.server = server;
        this.rootPackage = rootPackage;
    }

    //~ Methods ----------------------------------------------------------------

    RefPackage lookupRefPackage(
        String [] names,
        int prefix)
    {
        if (server.schemaName != null) {
            if (prefix > 0) {
                assert (prefix == 1);
                assert (names[0].equals(server.schemaName));
            }
            return rootPackage;
        }
        return JmiObjUtil.getSubPackage(rootPackage, names, prefix);
    }

    /**
     * Looks up a RefClass from its qualified name.
     *
     * @param foreignName name of this class relative to this directory's root
     * package
     */
    RefClass lookupRefClass(String [] foreignName)
    {
        int prefix = foreignName.length - 1;
        RefPackage refPackage = lookupRefPackage(foreignName, prefix);
        if (refPackage == null) {
            return null;
        }
        try {
            return refPackage.refClass(foreignName[prefix]);
        } catch (InvalidNameException ex) {
            return null;
        }
    }

    RefBaseObject lookupRefBaseObject(String [] foreignName)
    {
        int prefix = foreignName.length - 2;
        RefPackage refPackage = lookupRefPackage(foreignName, prefix);
        if (refPackage == null) {
            return null;
        }
        try {
            String name = foreignName[prefix];
            String type = foreignName[prefix + 1];
            if (type.equals("Class")) {
                return refPackage.refClass(name);
            } else if (type.equals("Association")) {
                return refPackage.refAssociation(name);
            } else {
                assert (type.equals("Package"));
                return refPackage.refPackage(name);
            }
        } catch (InvalidNameException ex) {
            return null;
        }
    }

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        return lookupColumnSetAndImposeType(
            typeFactory,
            new String[] { foreignName },
            localName,
            null);
    }

    FarragoMedColumnSet lookupColumnSetAndImposeType(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName,
        RelDataType rowType)
        throws SQLException
    {
        RefClass refClass = lookupRefClass(foreignName);
        if (refClass == null) {
            return null;
        }

        if (rowType == null) {
            rowType = computeRowType(typeFactory, refClass);
        }

        return new MedMdrClassExtent(
            this,
            typeFactory,
            refClass,
            foreignName,
            localName,
            rowType);
    }

    private RelDataType computeRowType(
        FarragoTypeFactory typeFactory,
        RefClass refClass)
    {
        List<StructuralFeature> features =
            JmiObjUtil.getFeatures(refClass, StructuralFeature.class, false);
        int n = features.size();
        RelDataType [] types = new RelDataType[n + 2];
        String [] fieldNames = new String[n + 2];
        for (int i = 0; i < n; ++i) {
            StructuralFeature feature = features.get(i);
            fieldNames[i] = feature.getName();
            types[i] = typeFactory.createMofType(feature);
            if (server.foreignRepository) {
                // for foreign repositories, we don't have generated
                // classes, so need to treat everything as nullable
                types[i] =
                    typeFactory.createTypeWithNullability(types[i], true);
            }
        }

        // TODO jvs 15-Mar-2004: We currently generate a VARCHAR type for the
        // mofId.  This means that if we want to reference the object again
        // later, we have to look it up via MDRepository.getByMofId.  This will
        // always be required in cases where the object has to leave and
        // re-enter Java (e.g. through Fennel or over the network), but within a
        // contiguous series of Java XO's, it would be better to keep an object
        // reference instead.  So, we need to enhance Farrago's type system to
        // include Serializable object types and use the MOFID for
        // serialization.  This would also allow us to return direct repository
        // references to embedded JDBC clients (and remote JDBC clients with
        // access to a repository shared with Farrago).
        fieldNames[n] = "mofId";
        types[n] = typeFactory.createMofType(null);
        fieldNames[n + 1] = "mofClassName";
        types[n + 1] = typeFactory.createMofType(null);
        return typeFactory.createStructType(types, fieldNames);
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        RefPackage subPackage =
            lookupRefPackage(
                new String[] { foreignName },
                1);
        if (subPackage == null) {
            return null;
        }
        return new MedMdrNameDirectory(server, subPackage);
    }

    // TODO:  queryMetadata
}

// End MedMdrNameDirectory.java
