/*
// $Id$
// Aspen dataflow server
// Copyright (C) 2004-2004 Disruptive Tech
*/

package net.sf.farrago.query;

import org.eigenbase.relopt.RelOptPlanner;

/**
 * FarragoPlanner represents a query planner/optimizer associated with
 * a specific FarragoPreparingStmt.
 *
 * @author stephan
 * @version $Id$
 */
public interface FarragoPlanner
    extends RelOptPlanner
{
    /**
     * @return the FarragoPreparingStmt associated with this planner.
     */
    public FarragoPreparingStmt getPreparingStmt();
}
