/*
// $Id$
// Aspen dataflow server
// Copyright (C) 2004-2004 Disruptive Tech
*/

package net.sf.farrago.session;

import org.eigenbase.relopt.RelOptPlanner;

/**
 * FarragoSessionPlanner represents a query planner/optimizer associated with
 * a specific FarragoPreparingStmt.
 *
 * @author stephan
 * @version $Id$
 */
public interface FarragoSessionPlanner
    extends RelOptPlanner
{
    /**
     * @return the FarragoSessionPreparingStmt associated with this planner.
     */
    public FarragoSessionPreparingStmt getPreparingStmt();
}
