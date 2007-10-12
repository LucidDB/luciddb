/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.catalog;

import java.io.*;

import java.net.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;
import javax.jmi.xmi.*;

import net.sf.farrago.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.JmiObjUtil;
import org.eigenbase.util.*;

import org.netbeans.api.mdr.*;
import org.netbeans.api.xmi.*;
import org.netbeans.mdr.*;
import org.netbeans.mdr.persistence.*;


/**
 * Static utilities for manipulating the Farrago repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoReposUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Exports a submodel, generating qualified references by name to objects
     * outside of the submodel.
     *
     * @param mdrRepos MDR repository containing submodel to export
     * @param outputFile file into which XMI should be written
     * @param subPackageName name of package containing submodel to be exported
     */
    public static void exportSubModel(
        MDRepository mdrRepos,
        File outputFile,
        String subPackageName)
        throws Exception
    {
        XMIWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        ExportRefProvider refProvider =
            new ExportRefProvider(
                subPackageName);
        xmiWriter.getConfiguration().setReferenceProvider(refProvider);
        FileOutputStream outStream = new FileOutputStream(outputFile);
        try {
            xmiWriter.write(
                outStream,
                "SUBMODEL",
                mdrRepos.getExtent("FarragoMetamodel"),
                "1.2");
            if (!refProvider.subPackageFound) {
                throw new NoSuchElementException(subPackageName);
            }
        } finally {
            outStream.close();
        }
    }

    public static void importSubModel(
        MDRepository mdrRepos,
        URL inputUrl)
        throws Exception
    {
        XMIReader xmiReader = XMIReaderFactory.getDefault().createXMIReader();
        ImportRefResolver refResolver =
            new ImportRefResolver(
                (Namespace) mdrRepos.getExtent("FarragoCatalog")
                                    .refMetaObject());
        xmiReader.getConfiguration().setReferenceResolver(refResolver);
        boolean rollback = false;
        try {
            mdrRepos.beginTrans(true);
            rollback = true;
            xmiReader.read(
                inputUrl.toString(),
                mdrRepos.getExtent("FarragoMetamodel"));
            rollback = false;
            mdrRepos.endTrans();
        } finally {
            if (rollback) {
                mdrRepos.endTrans(true);
            }
        }
    }

    public static void dumpRepository()
        throws Exception
    {
        dumpRepository(new FarragoModelLoader());
    }

    public static void dumpRepository(
        FarragoModelLoader modelLoader)
        throws Exception
    {
        FarragoProperties farragoProps = modelLoader.getFarragoProperties();
        File catalogDir = farragoProps.getCatalogDir();
        File metamodelDump = new File(catalogDir, "FarragoMetamodelDump.xmi");
        File catalogDump = new File(catalogDir, "FarragoCatalogDump.xmi");

        boolean success = false;
        try {
            FarragoPackage farragoPackage =
                modelLoader.loadModel("FarragoCatalog", false);
            exportExtent(
                modelLoader.getMdrRepos(),
                metamodelDump,
                "FarragoMetamodel");
            exportExtent(
                modelLoader.getMdrRepos(),
                catalogDump,
                "FarragoCatalog");
            deleteStorage(modelLoader, farragoPackage);
            success = true;
        } finally {
            modelLoader.close();
            if (!success) {
                metamodelDump.delete();
                catalogDump.delete();
            }
        }
    }

    /**
     * @deprecated pass FarragoModelLoader parameter
     */
    public static boolean isReloadNeeded()
    {
        return isReloadNeeded(new FarragoModelLoader());
    }

    public static boolean isReloadNeeded(FarragoModelLoader modelLoader)
    {
        File catalogDir = modelLoader.getFarragoProperties().getCatalogDir();
        return new File(catalogDir, "FarragoMetamodelDump.xmi").exists();
    }

    /**
     * @deprecated pass FarragoModelLoader parameter
     */
    public static void reloadRepository()
        throws Exception
    {
        reloadRepository(new FarragoModelLoader());
    }

    public static void reloadRepository(FarragoModelLoader modelLoader)
        throws Exception
    {
        File catalogDir = modelLoader.getFarragoProperties().getCatalogDir();
        File metamodelDump = new File(catalogDir, "FarragoMetamodelDump.xmi");
        File catalogDump = new File(catalogDir, "FarragoCatalogDump.xmi");

        try {
            modelLoader.initStorage(false);

            // import metamodel
            importExtent(
                modelLoader.getMdrRepos(),
                metamodelDump,
                "FarragoMetamodel",
                null,
                null);

            // import catalog
            importExtent(
                modelLoader.getMdrRepos(),
                catalogDump,
                "FarragoCatalog",
                "FarragoMetamodel",
                "Farrago");

            metamodelDump.delete();
            catalogDump.delete();
        } finally {
            modelLoader.close();
        }
    }

    public static void exportExtent(
        MDRepository mdrRepos,
        File file,
        String extentName)
        throws Exception
    {
        RefPackage refPackage = mdrRepos.getExtent(extentName);
        XmiWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        FileOutputStream outStream = new FileOutputStream(file);
        try {
            xmiWriter.write(outStream, refPackage, "1.2");
        } finally {
            outStream.close();
        }
    }

    private static void deleteStorage(
        FarragoModelLoader modelLoader,
        FarragoPackage farragoPackage)
        throws Exception
    {
        try {
            // grotty internals for dropping physical repos storage
            String mofIdString = farragoPackage.refMofId();
            MOFID mofId = MOFID.fromString(mofIdString);

            NBMDRepositoryImpl reposImpl =
                (NBMDRepositoryImpl) modelLoader.getMdrRepos();
            Storage storage =
                reposImpl.getMdrStorage().getStorageByMofId(mofId);
            storage.close();
            storage.delete();
        } finally {
            modelLoader.close();
        }
    }

    private static void importExtent(
        MDRepository mdrRepos,
        File file,
        String extentName,
        String metaPackageExtentName,
        String metaPackageName)
        throws Exception
    {
        RefPackage extent;
        if (metaPackageExtentName != null) {
            ModelPackage modelPackage =
                (ModelPackage) mdrRepos.getExtent(metaPackageExtentName);
            MofPackage metaPackage = null;
            for (Object o : modelPackage.getMofPackage().refAllOfClass()) {
                MofPackage result = (MofPackage) o;
                if (result.getName().equals(metaPackageName)) {
                    metaPackage = result;
                    break;
                }
            }
            extent = mdrRepos.createExtent(extentName, metaPackage);
        } else {
            extent = mdrRepos.createExtent(extentName);
        }
        XmiReader xmiReader = XMIReaderFactory.getDefault().createXMIReader();
        boolean rollback = false;
        try {
            mdrRepos.beginTrans(true);
            rollback = true;
            xmiReader.read(
                file.toURL().toString(),
                extent);
            rollback = false;
            mdrRepos.endTrans();
        } finally {
            if (rollback) {
                mdrRepos.endTrans(true);
            }
        }
    }

    private static void mainExportSubModel(String [] args)
        throws Exception
    {
        assert (args.length == 3);
        File file = new File(args[1]);
        String subPackageName = args[2];
        FarragoModelLoader modelLoader = new FarragoModelLoader();
        try {
            modelLoader.loadModel("FarragoCatalog", false);
            exportSubModel(
                modelLoader.getMdrRepos(),
                file,
                subPackageName);
        } finally {
            modelLoader.close();
        }
    }

    private static void mainImportSubModel(String [] args)
        throws Exception
    {
        assert (args.length == 2);
        File file = new File(args[1]);
        FarragoModelLoader modelLoader = new FarragoModelLoader();
        try {
            modelLoader.loadModel("FarragoCatalog", false);
            importSubModel(
                modelLoader.getMdrRepos(),
                file.toURL());
        } finally {
            modelLoader.close();
        }
    }

    public static void main(String [] args)
        throws Exception
    {
        // TODO:  proper arg checking
        assert (args.length > 0);
        if (args[0].equals("exportSubModel")) {
            mainExportSubModel(args);
        } else if (args[0].equals("importSubModel")) {
            mainImportSubModel(args);
        } else {
            throw new IllegalArgumentException(args[0]);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ExportRefProvider
        implements XMIReferenceProvider
    {
        private final String subPackageName;

        boolean subPackageFound;

        ExportRefProvider(
            String subPackageName)
        {
            this.subPackageName = subPackageName;
        }

        // implement XMIReferenceProvider
        public XMIReferenceProvider.XMIReference getReference(RefObject obj)
        {
            RefObject parent = obj;
            if (obj instanceof Tag) {
                Collection c = ((Tag) obj).getElements();
                if (c.size() == 1) {
                    parent = (RefObject) c.iterator().next();
                }
            }
            List<String> nameList = new ArrayList<String>();
            do {
                String name = (String) parent.refGetValue("name");
                nameList.add(name);
                if (subPackageName.equals(name)) {
                    subPackageFound = true;
                    return new XMIReferenceProvider.XMIReference(
                        "SUBMODEL",
                        Long.toString(JmiObjUtil.getObjectId(obj)));
                }
                parent = (RefObject) parent.refImmediateComposite();
            } while (parent != null);
            Collections.reverse(nameList);
            int k = 0;
            StringBuilder sb = new StringBuilder();
            for (String name : nameList) {
                if (k++ > 0) {
                    sb.append('/');
                }
                sb.append(name);
            }
            return new XMIReferenceProvider.XMIReference(
                "REPOS",
                sb.toString());
        }
    }

    private static class ImportRefResolver
        implements XMIReferenceResolver
    {
        private final Namespace root;

        ImportRefResolver(Namespace root)
        {
            this.root = root;
        }

        // implement XMIReferenceResolver
        public void register(
            String systemId,
            String xmiId,
            RefObject object)
        {
            // don't care
        }

        // implement XMIReferenceResolver
        public void resolve(
            XMIReferenceResolver.Client client,
            RefPackage extent,
            String systemId,
            XMIInputConfig configuration,
            Collection hrefs)
        {
            Iterator iter = hrefs.iterator();
            while (iter.hasNext()) {
                String href = iter.next().toString();
                int nameStart = href.indexOf('#') + 1;
                assert (nameStart != 0);
                String [] names = href.substring(nameStart).split("/");
                assert (names[0].equals(root.getName()));
                Namespace ns = root;
                try {
                    for (int i = 1; i < (names.length - 1); ++i) {
                        ns = (Namespace) ns.lookupElement(names[i]);
                    }
                    ModelElement element;
                    if (names.length == 1) {
                        element = (ModelElement) ns;
                    } else {
                        element = ns.lookupElement(names[names.length - 1]);
                    }
                    client.resolvedReference(href, element);
                } catch (NameNotFoundException ex) {
                    throw Util.newInternal(ex);
                }
            }
        }
    }
}

// End FarragoReposUtil.java
