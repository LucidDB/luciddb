/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.jmi;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.Map.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.type.*;
import org.eigenbase.util.*;

/**
 * Base utility class for converting between JMI reference objects and their
 * JSON representation and back. This is loosely meant to corespond to the
 * class {@link JmiObjUtil}, which performs those functions using XMI as its
 * interchange format.<p>
 *
 * The main entry points in the class are the export and import methods,
 * {@link #generateJSON(Collection)} and
 * {@link #importFromJsonString(RefPackage, String)}, respectively.
 *
 * @author chard
 *
 */
public abstract class JmiJsonUtil
{
    //~ Instance fields -------------------------------------------------------

    private static final Logger tracer =
        Logger.getLogger(JmiJsonUtil.class.toString());
    private final Map<String, RefObject> loadCache =
        new HashMap<String, RefObject>();
    private final Map<String, String> serverMofIds =
        new HashMap<String, String>();
    private Map<String, List<Pair<String, String>>> unresolvedReferences =
        new HashMap<String, List<Pair<String, String>>>();
    private Map<String, List<String>> referenceKeys =
        new HashMap<String, List<String>>();

    protected JmiJsonUtil()
    {
    }

    // JSON metadata methods
    /**
     * Imports a collection of JMI objects from their JSON representations.
     * This is the analog of
     * {@link JmiObjUtil#importFromXmiString(RefPackage, String)}, but using
     * JSON for the interchange. It is the inverse operation of
     * {@link #generateJSON(Collection)}.
     *
     * @param target RefPackage (generally the farrago package) that will hold
     * the imported objects
     * @param jsonString String containing a list of JSON representations of
     * JMI objects
     * @return Collection of imported JMI objects reconstituted from their JSON
     * representations
     */
    public Collection<RefBaseObject> importFromJsonString(
        RefPackage target,
        String jsonString)
    {
        List<RefBaseObject> result = new LinkedList<RefBaseObject>();
        List<JsonMetadataBean> beans = new LinkedList<JsonMetadataBean>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<JsonMetadataBean> foo =
                mapper.readValue(
                    jsonString,
                    new TypeReference<List<JsonMetadataBean>>() {
                    });
            beans.addAll(foo);
            for (JsonMetadataBean bean : beans) {
                fixAttributes(bean, target);
                bean.fixAttributes();
                RefObject object = createObject(bean, target);
                result.add(object);
                String objectKey = getObjectKey(object);
                loadCache.put(objectKey, object);
                tracer.fine(
                    "MOF stored for " + objectKey + ": " + bean.getMofId());
                serverMofIds.put(objectKey, bean.getMofId());
            }
        } catch (JsonProcessingException e) {
            tracer.severe("Problem parsing JSON: " + e.getMessage());
            tracer.severe("Stack trace: " + Util.getStackTrace(e));
        } catch (IOException e) {
            tracer.severe("Problem reading JSON: " + e.getMessage());
            tracer.severe("Stack trace: " + Util.getStackTrace(e));
        }
        return result;
    }

    /**
     * Callout routine called after each JsonMetadataBean is built from the
     * JSON, but before the bean is used to create a JMI object. This allows
     * a subclass to process attributes that cannot be handled natively as
     * string values. For example, attributes that are expressions arrive from
     * JSON as Map instances that need to be reconstituted into the proper
     * CwmExpression subclasses.
     *
     * @param bean JsonMetadataBean representing an object mapped from JSON
     * @param target RefPackage (probably te farrago package) that will contain
     * the rebuilt JMI reference object
     */
    protected abstract void fixAttributes(
        JsonMetadataBean bean,
        RefPackage target);

    /**
     * Generates a unique key for the given RefObject. The default
     * implementation, which is probably never useful in a real application,
     * simply concatenates the object's name and type, separated by a dot.
     * Subclasses will generally want to add namespace information.
     * @param object RefObject for which to generate a unique ID
     * @return String containing a unique identifier for the object
     */
    public String getObjectKey(RefObject object)
    {
        tracer.severe("Somehow, we got here. ZZZZZ");
        String result =
            object.refGetValue("name").toString()
            .concat(".")
            .concat(JmiObjUtil.getTypeName(object));
        return result;
    }

    /**
     * COnstructs and returns a RefObject based on the metadata passed in the
     * JSON bean. Uses the target package (generally the farrgo package) to
     * construct the appropriate type of object based on the type information
     * in the JSON bean.
     * @param bean JsonMetadataBean instance representing the remote object's
     * attributes and references
     * @param target RefPackage that will hold and manage the imported object,
     * which probably also knows how to construct appropriate types of
     * RefObjects
     * @return RefObject corresponding to the input data
     */
    protected abstract RefObject createElementInstance(
        JsonMetadataBean bean,
        RefPackage target);

    /**
     * Constructs and populates a JMI object in the target package (most likely
     * the farrago package) from the description in the metadata parsed from a
     * JSON object.
     *
     * @param bean JsonMetadataBean containing the parsed attributes and
     * references for the new object
     * @param target RefPackage (generally assumed to be the farrago package)
     * that will hold the imported object
     * @return RefObject described by the JSON metadata
     */
    protected RefObject createObject(
        JsonMetadataBean bean,
        RefPackage target)
    {
        // Create the empty, named model element
        RefObject result = createElementInstance(bean, target);
        // cache the list of reference keys, if not already there
        if (!referenceKeys.containsKey(bean.getType())) {
            referenceKeys.put(
                bean.getType(),
                getReferenceKeys(result.refClass()));
        }
        // Handle attributes
        JmiObjUtil.setAttributeValues(result, bean.getAttributes());

        // Set references, too
        handleReferences(result, bean, target);
        return result;
    }

    /**
     * Resolves all references (or queues them up for later resolution) in the
     * imported object. References come in as either a single string or a list
     * of strings, each string being the key to a referenced object. If the
     * referenced object has already been imported, we resolve the reference. If
     * the referenced object isn't yet loaded, we add the reference to a list
     * for subsequent resolution.
     *
     * @param importedObject RefObject being generated from imported JSON
     * @param bean JsonMetadataBean parsed from the incoming JSON
     * @param target RefPackage that will hold the new object and any
     * objects we need to create in this process
     */
    protected abstract void handleReferences(
        RefObject importedObject,
        JsonMetadataBean bean,
        RefPackage target);

    /**
     * Resolves a reference between two CWM objects
     * @param refKey String containing the name of the reference (e.g.,
     * &quot;namespace&quot;
     * @param target The object being referred to
     * @param referrer The object referring to the target
     * @param targetPackage RefPackage (probably the farrago package) that
     * contains the JMI objects
     */
    protected abstract void resolveReference(
        String refKey,
        RefObject target,
        RefObject referrer,
        RefPackage targetPackage);

    /**
     * Converts a reference to an object (or collection of objects) into a
     * representation that can be passed in JSON (which generally means a
     * unique ID string or ist of same).
     *
     * @param exportObject RefObject containing the reference to flatten
     * @param sf StructuralFeature (and more particularly, a {@link Reference})
     * that we need to flatten
     * @param refs Map containing all flattened references for the object, to
     * which we add the flattened reference
     */
    protected void flattenReference(
        RefObject exportObject,
        StructuralFeature sf,
        Map<String, Object> refs)
    {
        String refName = sf.refGetValue("name").toString();
        Object refObject = null;
        Object reference = exportObject.refGetValue(sf);
        if (reference instanceof Collection) {
            // iterate the collection
            Collection<RefObject> refColl = (Collection<RefObject>) reference;
            if (refColl.isEmpty()) {
                return;
            }
            List<String> flatRefs = new LinkedList<String>();
            for (RefObject refObj : refColl) {
                flatRefs.add((getObjectKey(refObj)));
            }
            refObject = flatRefs;
        } else if (reference instanceof RefObject) {
            refObject = getObjectKey((RefObject) reference);
        } else {
            tracer.warning(
                "Don't know how to flatten reference of type "
                + sf.getClass().getName());
            refObject = sf.toString();
        }
        refs.put(refName, refObject);
    }

    /**
     * Generates a lit of JSON representations of the input objects. This is
     * the JSON analog of {@link JmiObjUtil#exportToXmiString(Collection)}.
     * @param results Collection of RefObjects to export.
     * @return String containing a list of JSON representations of the input
     * objects
     */
    public String generateJSON(Collection<RefObject> results)
    {
        List<Map<String, Object>> collection =
            new LinkedList<Map<String, Object>>();
        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        SortedMap<String, Object> object;
        mapper.configure(
            SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS,
            false);
        try {
            for (RefObject ro : results) {
                object = new TreeMap<String, Object>();
                object.put("name", ro.refGetValue("name"));
                object.put("mofId", ro.refMofId());
                object.put("type", JmiObjUtil.getTypeName(ro));

                Map<String, Object> attrs = JmiObjUtil.getAttributeValues(ro);
                flattenEnumAttrs(ro, attrs);
                object.put("attributes", attrs);

                SortedMap<String, Object> refs = new TreeMap<String, Object>();
                List<Reference> features =
                    JmiObjUtil.getFeatures(
                        ro.refClass(),
                        Reference.class,
                        true);
                for (StructuralFeature sf : features) {
                    if (ro.refGetValue(sf) == null) {
                        continue;
                    }
                    flattenReference(ro, sf, refs);
                }
                // move any refs that are really attrs (blame the model)
                moveReferenceAttributes(attrs, refs);
                if (!refs.isEmpty()) {
                    object.put("references", refs);
                }
                collection.add(object);
            }
            result = mapper.writeValueAsString(collection);
        } catch (Throwable e) {
            tracer.warning("Problem writing object to JSON: " + e.getMessage());
            tracer.warning("Stack trace: " + Util.getStackTrace(e));
        }
        return result;
    }

    /**
     * Moves &quot; references&quot; that don't actually refer to another
     * catalog object into the attributes list where they belong. This is a
     * workaround to the fact that some attributes (notably, iniial values for
     * clumns and parameters) are of type Reference, when they aren't really
     * references. This probably reflects an error in the model, but we can
     * work around it.
     *
     * @param attrs Map of object attributes and their values
     * @param refs Map of object references and their values
     */
    protected void moveReferenceAttributes(
        Map<String, Object> attrs,
        SortedMap<String, Object> refs)
    {
        Iterator<Entry<String, Object>> li = refs.entrySet().iterator();
        while (li.hasNext()) {
            Entry<String, Object> entry = li.next();
            if (entry.getValue() instanceof Map) {
                attrs.put(entry.getKey(), entry.getValue());
                li.remove();
            }
        }
    }

    /**
     * Flattens attribute values that are enumerations, since Jackson doesn't
     * understand all the references around the enums. So we just write the
     * {@link RefEnum} type and value and reconstitute it at the other end.
     * @param ro RefObject we want to turn into JSON
     * @param attrs Map containing the attributes from the RefObject
     */
    protected void flattenEnumAttrs(
        RefObject ro,
        Map<String, Object> attrs)
    {
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (value instanceof RefEnum) {
                RefEnum refEnum = (RefEnum) value;
                Map<String, Object> enumRef = new HashMap<String, Object>();
                enumRef.put("enumType", refEnum.getClass().getName());
                enumRef.put("enumValue", value.toString());
                entry.setValue(enumRef);
            }
        }
    }

    /**
     * Restores an enumeration-type attribute flattened by
     * {@link #flattenEnumAttrs(RefObject, Map)}.
     *
     * @param attrMap Map containing type and value information for the
     * attribute
     * @return RefEnum of the proper type and value
     */
    public static RefEnum unflattenEnumAttrs(Map<String, String> attrMap)
    {
        RefEnum refEnum = null;
        try {
            Class<? extends RefEnum> cls = Class.forName(
                attrMap.get("enumType")).asSubclass(RefEnum.class);
            Method method = cls.getMethod("forName", String.class);
            refEnum = cls.cast(method.invoke(null, attrMap.get("enumValue")));
        } catch (Exception e) {
            tracer.severe("Exception flattening attribute " + e.getMessage());
            tracer.severe("Stack trace: " + Util.getStackTrace(e));
            e.printStackTrace();
        }
        return refEnum;
    }

    /**
     * Returns an already-loaded remote object from the cache. If the object is
     * not in the cache, returns null.
     * @param key String containing the object key representing the desired
     * object
     * @return RefObject coresponding to the object key, if that object has
     * already loaded; otherwise, null
     */
    public RefObject getLoadedObject(String key)
    {
        return loadCache.get(key);
    }

    /**
     * Looks up the original MOFID of an object imported from the server. This
     * is needed for making MED service calls back to the server, which are
     * keyed by MOFID. Clients may need to know the MOFID of the object as it
     * exists on the server, such as when making remote SQL/MED calls.
     *
     * @param key String containing the object key (i.e., qualified name) of an
     * object in the load cache
     * @return MOFID of the server object with the same object key
     */
    public String getServerMofId(String key)
    {
        return serverMofIds.get(key);
    }

    /**
     * Retrieves the list of unresolved references for a given reference key.
     * The reference key is the name of a reference as it appears in the JSON
     * representation of a remote object.
     * @param key String containing the reference key
     * @return List of unresolved references of the given type
     */
    public List<Pair<String, String>> getUnresolvedReferences(String key)
    {
        List<Pair<String, String>> result = unresolvedReferences.get(key);
        if (result == null) {
            result = new LinkedList<Pair<String, String>>();
            unresolvedReferences.put(key, result);
        }
        return result;
    }

    /**
     * Adds a reference to a not-yet-loaded object to the appropriate list for
     * future resolution.
     * @param key String containing the reference key (i.e., the name of the
     * type of reference
     * @param refTo String containing the object key for the object referred to,
     * which is the one not yet loaded
     * @param refFrom String containing the object key of the object referring
     * to the not-yet-loaded object
     */
    public void addUnresolvedReference(String key, String refTo, String refFrom)
    {
        getUnresolvedReferences(key).add(
            new Pair<String, String>(refTo, refFrom));
        if (tracer.isLoggable(Level.FINE)) {
            int keyCount = unresolvedReferences.size();
            int refCount = 0;
            for (
                List<Pair<String, String>> list
                : unresolvedReferences.values())
            {
                refCount += list.size();
            }
            tracer.fine(
                " Unresolved: " + refCount + " items in "
                + keyCount + " lists");
        }
    }

    private List<String> getReferenceKeys(RefClass refClass)
    {
        List<Reference> refs =
            JmiObjUtil.getFeatures(refClass, Reference.class, true);
        List<String> result = new LinkedList<String>();
        for (Reference ref : refs) {
            result.add(ref.getName());
        }
        // "namespace" needs to be first reference in list
        int nsSlot = result.indexOf("namespace");
        if (nsSlot > 0) {
            result.add(0, result.remove(nsSlot));
        }
        return result;
    }

    /**
     * Returns the list of reference key names for a given type of object. This
     * enables the reference resolver to know which kinds of references to look
     * for and handle for a given object. Note that if the reference key
     * &quot;namespace&quot; appears in the list, it will always come first, as
     * the namespace is used to generate the object key for the object.
     * @param typeName String containing the model type name of an imported
     * remote object. So this is, for example, &quot;LocalCatalog&quot;, not
     * &quot;FemLocalCatalog&quot;.
     * @return List of String objects representing the reference types that
     * might be present in the given object type
     */
    protected List<String> getReferenceKeys(String typeName)
    {
        tracer.fine("Getting keys for type " + typeName);
        List<String> result = referenceKeys.get(typeName);
        if (result == null) {
            tracer.severe("This shouldn't ever happen! FAIL");
        }
        return result;
    }

    /**
     * Preloads the loaded-object cache with the list of objects
     * @param inputList
     */
    public void loadSqlDataTypes(Iterable<? extends RefObject> inputList)
    {
        for (RefObject item : inputList) {
            loadCache.put(getObjectKey(item), item);
        }
    }

}

// End JmiJsonUtil.java
