/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.util;

import java.io.*;
import java.util.*;
import org.netbeans.mdr.*;
import org.netbeans.api.mdr.*;
import org.netbeans.mdr.persistence.btreeimpl.btreestorage.*;

// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have no dependencies on other Farrago code.

/**
 * Static MDR utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MdrUtil
{
    /**
     * Loads an MDRepository instance.
     *
     * @param storageFactoryClassName fully qualified name of the class
     * used to implement repository storage
     *
     * @param storageProps storage-specific properties
     * (without the MDRStorageProperty prefix)
     *
     * @return loaded repository
     */
    public static MDRepository loadRepository(
        String storageFactoryClassName,
        Properties storageProps)
    {
        Iterator iter;
        String classNameProp =
            "org.netbeans.mdr.storagemodel.StorageFactoryClassName";
        Properties sysProps = System.getProperties();
        Map savedProps = new HashMap();

        String storagePrefix = "MDRStorageProperty.";

        if (storageFactoryClassName == null) {
            storageFactoryClassName = BtreeFactory.class.getName();
        }

        if (storageFactoryClassName.equals(BtreeFactory.class.getName())) {
            // special case
            storagePrefix = "";
        }

        // save existing system properties first
        savedProps.put(
            classNameProp,
            sysProps.get(classNameProp));
        iter = storageProps.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String propName = storagePrefix + entry.getKey();
            savedProps.put(
                propName,
                sysProps.get(propName));
        }

        try {
            // set desired properties
            sysProps.put(classNameProp,storageFactoryClassName);
            iter = storageProps.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                sysProps.put(
                    storagePrefix + entry.getKey(),
                    entry.getValue());
            }

            // load repository
            return new NBMDRepositoryImpl();
        } finally {
            // restore saved system properties
            iter = savedProps.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                if (entry.getValue() == null) {
                    sysProps.remove(entry.getKey());
                } else {
                    sysProps.put(entry.getKey(),entry.getValue());
                }
            }
        }
    }
}

// End MdrUtil.java
