/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
