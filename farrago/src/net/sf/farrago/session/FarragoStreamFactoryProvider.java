/*
// $Id$
// Aspen dataflow server
// Copyright (C) 2004-2004 Disruptive Tech
*/

package net.sf.farrago.session;

/**
 * FarragoStreamFactoryProvider provides a mechanism by which one can register
 * factories for extension ExecutionStreams in Fennel.
 *
 * @author stephan
 * @version $Id$
 */
public interface FarragoStreamFactoryProvider
{
    /**
     * Register factories for extension ExecutionStreams in Fennel.
     *
     * @param hStreamGraph native handle to unprepared stream graph
     */
    void registerStreamFactories(long hStreamGraph);
}
