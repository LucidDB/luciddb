/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.rel.convert;

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.rel.SaffronRel;


/**
 * Generic implementation of {@link ConverterRule} which lets a {@link
 * ConverterFactory} do the work.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Jun 18, 2003
 */
public class FactoryConverterRule extends ConverterRule
{
    //~ Instance fields -------------------------------------------------------

    private final ConverterFactory factory;

    //~ Constructors ----------------------------------------------------------

    public FactoryConverterRule(
        ConverterFactory factory,
        CallingConvention inConvention)
    {
        super(SaffronRel.class,inConvention,factory.getConvention(),null);
        this.factory = factory;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isGuaranteed()
    {
        return true;
    }

    public SaffronRel convert(SaffronRel rel)
    {
        return factory.convert(rel);
    }
}


// End FactoryConverterRule.java
