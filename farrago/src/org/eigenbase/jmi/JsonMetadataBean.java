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

import java.util.*;
import java.util.Map.*;

/**
 * JsonMetadataBean is a loosely bean-flavored data structure representing
 * values read from a JSON string containing repository objects from a server.
 * The idea is to have a lightweight way to transfer descriptions of
 * repository objects to a client without needing an entire repository or
 * all the related objects that might not be of interest.<p>
 *
 * The bean consists of a few properties about the represented object (name,
 * MOFID, and type) and two collections of items: attributes and references.<p>
 *
 * Attributes are information about the object itself, and depend on the type
 * of the described object. They are essentially name-value pairs<p>
 *
 * References are just that, references to other objects in the package, which
 * have a name describing the relationship and a key to the referenced object.
 *
 * @author chard
 * @since Jul 27, 2011
 * @version $Id$
 */
public class JsonMetadataBean
{
    private String name;
    private String mofId;
    private String type;
    private SortedMap<String, Object> attributes;
    private SortedMap<String, Object> references;

    public JsonMetadataBean()
    {
        attributes = new TreeMap<String, Object>();
        references = new TreeMap<String, Object>();
    }

    public void fixAttributes()
    {
        // Fix flattened enums
        for (Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, String> attrMap =
                    (Map<String, String>) entry.getValue();
                if (attrMap.containsKey("enumType")) {
                    entry.setValue(JmiJsonUtil.unflattenEnumAttrs(attrMap));
                }
            }
        }
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getMofId()
    {
        return mofId;
    }

    public void setMofId(String mofId)
    {
        this.mofId = mofId;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public SortedMap<String, Object> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(SortedMap<String, Object> attributes)
    {
        this.attributes = attributes;
    }

    public SortedMap<String, Object> getReferences()
    {
        return references;
    }

    public void setReferences(SortedMap<String, Object> refs)
    {
        this.references = refs;
    }
}

// End JsonMetadataBean.java
