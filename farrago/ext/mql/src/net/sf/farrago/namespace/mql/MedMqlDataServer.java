/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.namespace.mql;

import java.sql.*;

import java.util.*;

import javax.sql.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * MedMqlDataServer provides an implementation of the {@link
 * FarragoMedDataServer} interface for MQL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMqlDataServer
    extends MedAbstractDataServer
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_URL = "URL";

    public static final String PROP_METAWEB_TYPE = "METAWEB_TYPE";

    public static final String PROP_UDX_SPECIFIC_NAME = "UDX_SPECIFIC_NAME";

    public static final String DEFAULT_URL
        = "http://api.freebase.com/api/service/mqlread";

    public static final String DEFAULT_UDX_SPECIFIC_NAME
        = "LOCALDB.METAWEB.MQL_QUERY";

    //~ Instance fields --------------------------------------------------------

    private MedAbstractDataWrapper wrapper;

    private String url;

    //~ Constructors -----------------------------------------------------------

    MedMqlDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
        this.wrapper = wrapper;
    }

    //~ Methods ----------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        url = props.getProperty(PROP_URL, DEFAULT_URL);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map<String, Properties> columnPropMap)
        throws SQLException
    {
        String udxSpecificName = getProperties().getProperty(
            PROP_UDX_SPECIFIC_NAME, DEFAULT_UDX_SPECIFIC_NAME);

        requireProperty(tableProps, PROP_METAWEB_TYPE);
        String metawebType = tableProps.getProperty(PROP_METAWEB_TYPE);
        return new MedMqlColumnSet(
            this,
            localName,
            rowType,
            metawebType,
            udxSpecificName);
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);

        // pushdown rules

        // case 1: projection on top of a filter (with push down projection)
        // ie: filtering on variables which are not in projection
        planner.addRule(
            new MedMqlPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class, new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            FilterRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    ProjectRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                            MedMqlTableRel.class) }) }) }),
                "proj on filter on proj"));

        // case 2: filter with push down projection
        // ie: proj only has values which are already in filter expression
        planner.addRule(
            new MedMqlPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class, new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MedMqlTableRel.class) }) }),
                "filter on proj"));

        // case 3: filter with no projection to push down.
        // ie: select *
        planner.addRule(
            new MedMqlPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(MedMqlTableRel.class) }),
                "filter"));

        // case 4: only projection, no filter
        planner.addRule(
            new MedMqlPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(MedMqlTableRel.class) }),
                "proj"));

        // case 5:  neither projection nor filter
        planner.addRule(
            new MedMqlPushDownRule(
                new RelOptRuleOperand(MedMqlTableRel.class),
                "none"));
    }

    MedAbstractDataWrapper getWrapper()
    {
        return wrapper;
    }

    public String getUrl()
    {
        return url;
    }
}

// End MedMqlDataServer.java
