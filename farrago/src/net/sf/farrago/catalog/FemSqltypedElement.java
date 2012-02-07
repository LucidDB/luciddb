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
package net.sf.farrago.catalog;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;


/**
 * This interface belongs in the UML model, but can't live there due to
 * metamodel problems. That's the short story.
 *
 * <p>You want the unabridged version? CWM declares a number of important
 * attributes on CwmColumn, but the same attributes are needed on other classes
 * such as FemRoutineParameter, and they aren't present on the base class
 * CwmSqlparameter. Validation code would like to be able to handle any kind of
 * SQL-typed object uniformly, so a common interface is required.
 * FemRoutineParameter can't inherit from CwmColumn (that kludgy approach leads
 * to strange MDR multiple-inheritance anomalies). And MDR doesn't support
 * generation of operations, so we can't declare a UML interface with abstract
 * methods.
 *
 * <p>So, we define this interface outside the model and use proxies to
 * implement it; see {@link FarragoCatalogUtil#toFemSqltypedElement}. In the
 * model, we define a placeholder interface {@link FemAbstractTypedElement}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FemSqltypedElement
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @see CwmColumn#getPrecision
     */
    public Integer getPrecision();

    /**
     * @see CwmColumn#setPrecision
     */
    public void setPrecision(Integer newValue);

    /**
     * @see CwmColumn#getScale
     */
    public Integer getScale();

    /**
     * @see CwmColumn#setScale
     */
    public void setScale(Integer newValue);

    /**
     * @see CwmColumn#getLength
     */
    public Integer getLength();

    /**
     * @see CwmColumn#setLength
     */
    public void setLength(Integer newValue);

    /**
     * @see CwmColumn#getCollationName
     */
    public String getCollationName();

    /**
     * @see CwmColumn#setCollationName
     */
    public void setCollationName(String newValue);

    /**
     * @see CwmColumn#getCharacterSetName
     */
    public String getCharacterSetName();

    /**
     * @see CwmColumn#setCharacterSetName
     */
    public void setCharacterSetName(String newValue);

    /**
     * @see CwmStructuralFeature#getType
     */
    public CwmClassifier getType();

    /**
     * @see CwmStructuralFeature#setType
     */
    public void setType(CwmClassifier newValue);

    /**
     * @return unproxied object
     */
    public FemAbstractTypedElement getModelElement();
}

// End FemSqltypedElement.java
