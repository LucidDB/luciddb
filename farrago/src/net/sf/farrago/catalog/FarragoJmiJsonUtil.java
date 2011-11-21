/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

import java.util.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

/**
 * Farrago-specific implementation of JmiJsonUtil.
 *
 * @author chard
 */
public class FarragoJmiJsonUtil
    extends JmiJsonUtil
{
    private static final Logger tracer =
        Logger.getLogger(FarragoJmiJsonUtil.class.toString());
    private static FarragoJmiJsonUtil instance;
    protected List<String> deferredReferences =
        Arrays.asList(new String[] {
            "parameter", "StorageOptions", "feature", "SampleDataset",
            "importedElement"
            });
    protected List<String> ignoredReferences =
        Arrays.asList(new String[] {"TagAnnotation", "RowCountStats",
            "constraint", "clientConstraint", "trigger", "usingTrigger",
            "specification", "optionScopeColumn", "Histogram", "KeyComponent",
            "ownedElement", "importer", "PathElement", "ColumnSet"
            });

    /* (non-Javadoc)
     * @see org.eigenbase.jmi.JmiJsonUtil#createElementInstance(org.eigenbase.jmi.JsonMetadataBean, javax.jmi.reflect.RefPackage)
     */
    @Override
    protected RefObject createElementInstance(
        JsonMetadataBean bean,
        RefPackage target)
    {
        FarragoPackage farragoPackage = (FarragoPackage) target;
        RefObject result = null;
        final String beanType = bean.getType();
        Sql2003Package sql2003 = farragoPackage.getFem().getSql2003();
        MedPackage medPkg = farragoPackage.getFem().getMed();
        if (beanType.equals("LocalCatalog")) {
            result =
                sql2003.getFemLocalCatalog().createFemLocalCatalog();
        } else if (beanType.equals("LocalSchema")) {
            result =
                sql2003.getFemLocalSchema().createFemLocalSchema();
        } else if (beanType.equals("DataServer")) {
            result = medPkg.getFemDataServer().createFemDataServer();
        } else if (beanType.equals("DataWrapper")) {
            result = medPkg.getFemDataWrapper().createFemDataWrapper();
        } else if (beanType.equals("LocalTable")) {
            result = medPkg.getFemLocalTable().createFemLocalTable();
        } else if (beanType.equals("ForeignTable")) {
            result = medPkg.getFemForeignTable().createFemForeignTable();
        } else if (beanType.equals("LocalView")) {
            result = sql2003.getFemLocalView().createFemLocalView();
        } else if (beanType.equals("Routine")) {
            result = sql2003.getFemRoutine().createFemRoutine();
        } else if (beanType.equals("Operation")) {
            result = farragoPackage.getCwm().getBehavioral().getCwmOperation()
                .createCwmOperation();
        } else if (beanType.equals("StoredColumn")) {
            result = medPkg.getFemStoredColumn().createFemStoredColumn();
        } else if (beanType.equals("ViewColumn")) {
            result = medPkg.getFemViewColumn().createFemViewColumn();
        } else if (beanType.equals("RoutineParameter")) {
            result =
                sql2003.getFemRoutineParameter().createFemRoutineParameter();
        } else if (beanType.equals("ColumnListRoutineParameter")) {
            result = sql2003.getFemColumnListRoutineParameter()
                .createFemColumnListRoutineParameter();
        } else if (beanType.equals("StorageOption")) {
            result = medPkg.getFemStorageOption().createFemStorageOption();
        } else if (beanType.equals("UniqueKeyConstraint")) {
            result = sql2003.getFemUniqueKeyConstraint()
                .createFemUniqueKeyConstraint();
        } else if (beanType.equals("PrimaryKeyConstraint")) {
            result = sql2003.getFemPrimaryKeyConstraint()
                .createFemPrimaryKeyConstraint();
        } else if (beanType.equals("KeyComponent")) {
            result = sql2003.getFemKeyComponent().createFemKeyComponent();
        } else if (beanType.equals("LocalIndexColumn")) {
            result =
                medPkg.getFemLocalIndexColumn().createFemLocalIndexColumn();
        } else if (beanType.equals("LocalIndex")) {
            result = medPkg.getFemLocalIndex().createFemLocalIndex();
        } else {
            tracer.warning("Unhandled object of type " + beanType);
        }
        return result;
    }

    @Override
    protected void fixAttributes(JsonMetadataBean bean, RefPackage target)
    {
        FarragoPackage farragoPackage = (FarragoPackage) target;
        final String beanType = bean.getType();
        tracer.fine("Bean is a " + beanType);
        if (beanType.equals("Routine")) {
            // create a body
            tracer.fine("Inflating body attribute for " + bean.getName());
            Map<String, Object> bodyMap =
                (Map<String, Object>) bean.getAttributes().get("body");
            CwmProcedureExpression body =
                farragoPackage.getCwm().getCore().getCwmProcedureExpression()
                    .createCwmProcedureExpression(
                        bodyMap.get("body").toString(),
                        bodyMap.get("language").toString());
            bean.getAttributes().put("body", body);
        }
        if (beanType.equals("LocalView")) {
            tracer.fine("Inflating query expression for " + bean.getName());
            Map<String, Object> bodyMap =
                (Map<String, Object>) bean.getAttributes().get(
                    "queryExpression");
            CwmQueryExpression body =
                farragoPackage.getCwm().getDataTypes().getCwmQueryExpression()
                    .createCwmQueryExpression(
                        bodyMap.get("body").toString(),
                        bodyMap.get("language").toString());
            bean.getAttributes().put("queryExpression", body);
        }
        if (beanType.equals("Parameter") || beanType.equals("StoredColumn")) {
            tracer.fine("Inflating initial value for " + bean.getName());
            Map<String, Object> defaultMap =
                (Map<String, Object>) bean.getAttributes().get(
                    "initialValue");
            if ((defaultMap != null) && !defaultMap.isEmpty()) {
                CwmExpression initValue =
                    farragoPackage.getCwm().getCore().getCwmExpression()
                        .createCwmExpression(
                            defaultMap.get("body").toString(),
                            defaultMap.get("language").toString());
                bean.getAttributes().put("initialValue", initValue);
            }
        }
    }

    /**
     * Constructs a fully-qualified name for the given object and returns it as
     * a String. This basically duplicates
     * {@link FarragoCatalogUtil#getQualifiedName(CwmModelElement)}, but
     * without the overhead of SqlIdentifier and all it brings with it.
     *
     * @param element CwmModelElement we want a qualified name for
     * @return String containing the fully-qualified name of the object
     */
    public static String getQualifiedName(CwmModelElement element){
        List<String> names = new ArrayList<String>(3);
        names.add(element.getName());
        for (
            CwmNamespace ns = element.getNamespace();
            ns != null;
            ns = ns.getNamespace())
        {
            names.add(ns.getName());
        }
        if (names.size() == 1) {    // shortcut for unqualified names
            return names.get(0);
        }
        Collections.reverse(names);
        StringBuilder qname = new StringBuilder(names.get(0));
        for (int i = 1; i < names.size(); i++) {
            qname.append(".").append(names.get(i));
        }
        return qname.toString();
    }

    // @see org.eigenbase.jmi.JmiJsonUtil#getObjectKey(javax.jmi.reflect.RefObject)
    @Override
    public String getObjectKey(RefObject object)
    {
        String baseKey = "";
        if (object instanceof CwmModelElement) {
            baseKey = getQualifiedName((CwmModelElement) object);
        } else if (object instanceof StructuralFeature) {
            StructuralFeature sf = (StructuralFeature) object;
            baseKey = object.refGetValue(sf).toString();
        } else if (object instanceof FemStorageOption) {
            FemStorageOption fso = (FemStorageOption) object;
            FemElementWithStorageOptions se = fso.getStoredElement();
            if ((se != null) && (se instanceof CwmModelElement)) {
                CwmModelElement cme = (CwmModelElement) se;
                baseKey = getQualifiedName(cme) + "." + fso.getName();
            } else {
                tracer.warning("Generating key for StorageOption with no element");
                baseKey = fso.getName();
            }
        } else {
            tracer.warning("Trying to get key for unexpected Object "
                + object.refClass().getClass().getName());
            baseKey = "UNKNOWN";
        }
        return baseKey.concat(".").concat(JmiObjUtil.getTypeName(object));
    }

    /**
     * Gets a singleton instance of the Farrago JMI/JSON utility.
     * @return instance of FarragoJmiJsonUtil
     */
    public static synchronized FarragoJmiJsonUtil getInstance()
    {
        if (instance == null) {
            instance = new FarragoJmiJsonUtil();
        }
        return instance;
    }

    @Override
    protected void flattenReference(
        RefObject exportObject,
        StructuralFeature sf,
        Map<String, Object> refs)
    {
        String featureName = sf.getName();
        if (deferredReferences.contains(featureName)
            || ignoredReferences.contains(featureName)) {
            return;
        }
        if (featureName.equals("clientDependency")) {
            Collection<CwmDependency> deps =
                (Collection<CwmDependency>) exportObject.refGetValue(sf);
            if (deps.isEmpty()) {
                return;
            }
            List<String> suppliers = new LinkedList<String>();
            for (CwmDependency x : deps) {
                for (CwmModelElement e : x.getSupplier()) {
                    suppliers.add(getObjectKey(e));
                }
                refs.put(featureName, suppliers);
            }
            return;
        } else if (featureName.equals("Server")) {
            if (exportObject instanceof FemBaseColumnSet) {
                String serverName = getObjectKey(
                    ((CwmModelElement) exportObject.refGetValue(sf)));
                refs.put(featureName, serverName);
            } else if (exportObject instanceof FemDataWrapper) {
                Collection<FemDataServer> servers =
                    (Collection<FemDataServer>) exportObject.refGetValue(sf);
                if (servers.isEmpty()) {
                    return;
                }
                List<String> serverList = new LinkedList<String>();
                for (FemDataServer fds : servers) {
                    serverList.add(getObjectKey(fds));
                }
                refs.put(featureName, serverList);
            } else {
                tracer.warning("Unexpected Server ref in a "
                    + exportObject.toString());
            }
            return;
        } else if (featureName.equals("initialValue")) {
            CwmExpression initValue =
                (CwmExpression) exportObject.refGetValue(sf);
            if (initValue != null) {
                Map<String, String> initHash = new HashMap<String, String>();
                initHash.put("body", initValue.getBody());
                initHash.put("language", initValue.getLanguage());
                refs.put(featureName, initHash);
                return;
            }
        } else {
            super.flattenReference(exportObject, sf, refs);
        }
    }

    @Override
    protected void handleReferences(
        RefObject importedObject,
        JsonMetadataBean bean,
        RefPackage target)
    {
        // First, handle non-CWM objects
        // So far, that just means StorageOptions
        if (importedObject instanceof FemStorageOption) {
            FemStorageOption fso = (FemStorageOption) importedObject;
            Object storedObject = bean.getReferences().get("StoredElement");
            if (storedObject != null) {
                FemElementWithStorageOptions element =
                    (FemElementWithStorageOptions) getLoadedObject(
                        storedObject.toString());
                if (element == null) {
                    String beanKey = storedObject.toString();
                    beanKey =
                        beanKey.substring(0, beanKey.lastIndexOf('.') + 1)
                        + fso.getName()
                        + "." + bean.getType();
                    tracer.fine("Marking unresolved "
                        + storedObject.toString() + " for " + beanKey);
                    addUnresolvedReference(
                        "StoredElement",
                        storedObject.toString(),
                        beanKey);
                } else {
                    tracer.fine("Connecting option " + fso.getName()
                        + " to " + element);
                    element.getStorageOptions().add(fso);
                    fso.setStoredElement(element);
                }
            }
            return;
        }
        // Now handle CwmModelElements
        CwmModelElement element = (CwmModelElement) importedObject;
        // This will eventually be the qualified name of the object, once we
        // handle the namespace (if any)
        String beanKey = null;
        for (String refKey : getReferenceKeys(bean.getType())) {
            Object refObj = bean.getReferences().get(refKey);
            if (refObj == null) {
                continue;
            }
            if (refObj instanceof String) { // single,simple reference
                String objectKey = (String) refObj;
                processReference(
                    refKey,
                    objectKey,
                    element,
                    bean,
                    beanKey,
                    target);
            } else if (refObj instanceof List) {    // list of references
                List<String> refList = (List<String>) refObj;
                for (String objectKey : refList) {
                    processReference(
                        refKey,
                        objectKey,
                        element,
                        bean,
                        beanKey,
                        target);
                }
            } else {
                tracer.warning("Unexpected value of reference " + refKey
                    + " is a " + refObj.toString());
            }
            // Then see if we can resolve unresolved refs
            List<Pair<String, String>> unresolved =
                getUnresolvedReferences(refKey);
            for (ListIterator<Pair<String, String>> li =
                unresolved.listIterator(); li.hasNext();) {
                Pair<String, String> pair = li.next();
                if (pair.getKey().equals(beanKey)) {
                    processReference(
                        refKey,
                        pair.getKey(),
                        element,
                        bean,
                        beanKey,
                        target);
                    li.remove();    // remove from list, now that resolved
                }
            }
        }
    }

    /**
     * Processes an individual reference from the JSON data and either resolves
     * it or queues it for later resolution.
     *
     * @param refKey String representing the name of the reference
     * @param objectKey String representing the key to the object referenced
     * @param element CwmModelElement that contains the reference
     * @param bean JsonMetadataBean that originally generated the element
     * @param beanKey String to hold the object key for the element, only
     * altered when refKey is &quot;namespace&quot;
     * @param target RefPackage to use for instantiating new objects (probably
     * the farrago package)
     */
    protected void processReference(
        String refKey,
        String objectKey,
        CwmModelElement element,
        JsonMetadataBean bean,
        String beanKey,
        RefPackage target)
    {
        RefObject reference = getLoadedObject(objectKey);
        if (reference == null) {
            if (refKey.equals("namespace")) {
                // Have to hack the object key for unresolved namespace
                beanKey = objectKey + "." + bean.getName()
                    + "." + bean.getType();
            }
            tracer.fine("Marking unresolved " + refKey + " " + objectKey
                + " in " + bean.getName());
            addUnresolvedReference(refKey, objectKey, beanKey);
        } else {
            tracer.fine(" Connecting " + refKey + " " + objectKey
                + " to " + bean.getName());
            resolveReference(refKey, reference, element, target);
            if (refKey.equals("namespace")) {
                beanKey = getObjectKey(element);
            }
        }
    }

    @Override
    protected void resolveReference(
        String refKey,
        RefObject target,
        RefObject referrer,
        RefPackage targetPackage)
    {
        CwmModelElement referencedElement = (CwmModelElement) target;
        CwmModelElement element = (CwmModelElement) referrer;
        if (refKey.equals("namespace")) {
            element.setNamespace((CwmNamespace) target);
            ((CwmNamespace) target).getOwnedElement().add(element);
        } else if (refKey.equals("clientDependency")) {
            Collection<CwmDependency> objDeps = element.getClientDependency();
            CwmDependency dependency =
                ((FarragoPackage) targetPackage).getCwm().getCore()
                    .getCwmDependency().createCwmDependency();
            dependency.getClient().add(element);
            dependency.getSupplier().add(referencedElement);
            objDeps.add(dependency);
        } else if (refKey.equals("owner")) {
            CwmClassifier owner = (CwmClassifier) referencedElement;
            ((CwmFeature) element).setOwner(owner);
            int index = owner.getFeature().indexOf(element);
            if (index > 0) { // "create or replace"
                owner.getFeature().set(index, (CwmFeature) element);
            } else {
                owner.getFeature().add((CwmFeature) element);
            }
        } else if (refKey.equals("behavioralFeature")) {
            ((CwmParameter) element).setBehavioralFeature((FemRoutine) target);
            List<CwmParameter> params = ((FemRoutine) target).getParameter();
            params.add((CwmParameter) element);
        } else if (refKey.equals("Server")) {
            if (element instanceof FemDataWrapper) {
                ((FemDataWrapper) element).getServer().add(
                    (FemDataServer) target);
                ((FemDataServer) target).setWrapper((FemDataWrapper) element);
            } else if (element instanceof FemBaseColumnSet) {
                ((FemBaseColumnSet) element).setServer((FemDataServer) target);
                ((FemDataServer) target).getColumnSet().add(
                    (FemBaseColumnSet) element);
            } else {
                tracer.warning("Unexpected reference Server in " + element);
            }
        } else if (refKey.equals("Wrapper")) {
            if (element instanceof FemDataServer) {
                ((FemDataServer) element).setWrapper((FemDataWrapper) target);
                ((FemDataWrapper) target).getServer().add(
                    (FemDataServer) element);
            } else {
                tracer.warning("Need to add wrapper to " + element.toString());
            }
        } else if (refKey.equals("type")) {
            if (element instanceof CwmColumn) {
                ((CwmColumn) element).setType((CwmClassifier) target);
            } else if (element instanceof CwmParameter) {
                ((CwmParameter) element).setType((CwmClassifier) target);
            }
        } else if (refKey.equals("Component")) {
            ((FemKeyComponent) target).setKeyConstraint(
                (FemAbstractKeyConstraint) element);
            ((FemAbstractKeyConstraint) element).getComponent().add(
                (FemKeyComponent) target);
        } else if (refKey.equals("KeyConstraint")) {
            ((FemKeyComponent) element).setKeyConstraint(
                (FemAbstractKeyConstraint) target);
            ((FemAbstractKeyConstraint) target).getComponent().add(
                (FemKeyComponent) element);
        } else if (refKey.equals("Attribute")) {
            ((FemKeyComponent) element).setAttribute(
                (FemAbstractAttribute) target);
            ((FemAbstractAttribute) target).getKeyComponent().add(
                (FemKeyComponent) element);
        } else if (refKey.equals("indexedFeature")) {
            if (element instanceof CwmStructuralFeature) {
                ((CwmIndexedFeature) target).setFeature(
                    (CwmStructuralFeature) element);
                // REVIEW: Is there a connection in the column???
            } else if (element instanceof CwmIndex) {
//                ((CwmIndex) element).getIndexedFeature().add((CwmIndexedFeature) target);
            }
        } else if (refKey.equals("index")) {
            ((CwmIndexedFeature) element).setIndex((CwmIndex) target);
        } else if (refKey.equals("spannedClass")) {
            ((CwmIndex) element).setSpannedClass((CwmClass) target);
            // How to link table back to index?
        } else if (refKey.equals("method")) {
            ((CwmOperation) element).getMethod().add((CwmMethod) target);
        } else {
            tracer.warning("Unhandled reference " + refKey
                + " needs resolution");
        }
    }

    @Override
    public String getLurqlClass(RefObject element)
    {
        String result = null;
        if (element instanceof FemDataServer) {
            result = "DataServer";
        } else if (element instanceof CwmCatalog) {
            result = "Catalog";
        } else if (element instanceof CwmSchema) {
            result = "Schema";
        } else if (element instanceof FemDataWrapper) {
            result = "DataWrapper";
        } else if (element instanceof FemLocalTable) {
            result = "Table";
        } else if (element instanceof FemForeignTable) {
            result = "ForeignTable";
        } else if (element instanceof FemLocalView) {
            result = "View";
        } else if (element instanceof FemRoutine) {
            result = "Routine";
        }
        return result;
    }

}

// End FarragoJmiJsonUtil.java
