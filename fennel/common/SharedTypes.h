/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#ifndef Fennel_SharedTypes_Included
#define Fennel_SharedTypes_Included

#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

// shared_ptr declarations to ease usage of handle/body pattern

class RandomAccessDevice;
typedef boost::shared_ptr<RandomAccessDevice> SharedRandomAccessDevice;

class Cache;
typedef boost::shared_ptr<Cache> SharedCache;

class CacheAccessor;
typedef boost::shared_ptr<CacheAccessor> SharedCacheAccessor;

class Segment;
typedef boost::shared_ptr<Segment> SharedSegment;

class SegmentAccessor;
typedef boost::shared_ptr<SegmentAccessor> SharedSegmentAccessor;

class SegmentFactory;
typedef boost::shared_ptr<SegmentFactory> SharedSegmentFactory;

class SegmentMap;
typedef boost::shared_ptr<SegmentMap> SharedSegmentMap;

class ByteOutputStream;
typedef boost::shared_ptr<ByteOutputStream> SharedByteOutputStream;

class ByteInputStream;
typedef boost::shared_ptr<ByteInputStream> SharedByteInputStream;

class ByteStreamMarker;
typedef boost::shared_ptr<ByteStreamMarker> SharedByteStreamMarker;

class ByteArrayOutputStream;
typedef boost::shared_ptr<ByteArrayOutputStream> SharedByteArrayOutputStream;

class ByteArrayInputStream;
typedef boost::shared_ptr<ByteArrayInputStream> SharedByteArrayInputStream;

class StatsSource;
typedef boost::shared_ptr<StatsSource> SharedStatsSource;

class StatsTarget;
typedef boost::shared_ptr<StatsTarget> SharedStatsTarget;

class SegOutputStream;
typedef boost::shared_ptr<SegOutputStream> SharedSegOutputStream;

class SegInputStream;
typedef boost::shared_ptr<SegInputStream> SharedSegInputStream;

class SpillOutputStream;
typedef boost::shared_ptr<SpillOutputStream> SharedSpillOutputStream;

class CheckpointProvider;
typedef boost::shared_ptr<CheckpointProvider> SharedCheckpointProvider;

class LogicalTxn;
typedef boost::shared_ptr<LogicalTxn> SharedLogicalTxn;

class LogicalTxnLog;
typedef boost::shared_ptr<LogicalTxnLog> SharedLogicalTxnLog;

class LogicalRecoveryLog;
typedef boost::shared_ptr<LogicalRecoveryLog> SharedLogicalRecoveryLog;

class LogicalTxnParticipant;
typedef boost::shared_ptr<LogicalTxnParticipant> SharedLogicalTxnParticipant;

class BTreeAccessBase;
typedef boost::shared_ptr<BTreeAccessBase> SharedBTreeAccessBase;

class BTreeReader;
typedef boost::shared_ptr<BTreeReader> SharedBTreeReader;

class BTreeWriter;
typedef boost::shared_ptr<BTreeWriter> SharedBTreeWriter;

class BTreeVerifier;
typedef boost::shared_ptr<BTreeVerifier> SharedBTreeVerifier;

class BTreeBuilder;
typedef boost::shared_ptr<BTreeBuilder> SharedBTreeBuilder;

class BTreeRecoveryFactory;
typedef boost::shared_ptr<BTreeRecoveryFactory> SharedBTreeRecoveryFactory;

class Database;
typedef boost::shared_ptr<Database> SharedDatabase;

class CheckpointThread;
typedef boost::shared_ptr<CheckpointThread> SharedCheckpointThread;

class ExecutionStream;
typedef boost::shared_ptr<ExecutionStream> SharedExecutionStream;

class ExecutionStreamParams;
typedef boost::shared_ptr<ExecutionStreamParams> SharedExecutionStreamParams;

class ExecutionStreamGraph;
typedef boost::shared_ptr<ExecutionStreamGraph> SharedExecutionStreamGraph;

class ExecutionStreamFactory;
typedef boost::shared_ptr<ExecutionStreamFactory> SharedExecutionStreamFactory;

class ExecStream;
typedef boost::shared_ptr<ExecStream> SharedExecStream;

class ExecStreamEmbryo;
typedef boost::shared_ptr<ExecStreamEmbryo> SharedExecStreamEmbryo;

class ExecStreamBufAccessor;
typedef boost::shared_ptr<ExecStreamBufAccessor> SharedExecStreamBufAccessor;

class ExecStreamScheduler;
typedef boost::shared_ptr<ExecStreamScheduler> SharedExecStreamScheduler;

class ExecStreamParams;
typedef boost::shared_ptr<ExecStreamParams> SharedExecStreamParams;

class ExecStreamGraph;
typedef boost::shared_ptr<ExecStreamGraph> SharedExecStreamGraph;

class ExecStreamFactory;
typedef boost::shared_ptr<ExecStreamFactory> SharedExecStreamFactory;

class TupleStream;
typedef boost::shared_ptr<TupleStream> SharedTupleStream;

class TupleStreamGraph;
typedef boost::shared_ptr<TupleStreamGraph> SharedTupleStreamGraph;

class TableWriter;
typedef boost::shared_ptr<TableWriter> SharedTableWriter;

class TableWriterFactory;
typedef boost::shared_ptr<TableWriterFactory> SharedTableWriterFactory;

class Calculator;
typedef boost::shared_ptr<Calculator> SharedCalculator;

FENNEL_END_NAMESPACE

#endif

// End SharedTypes.h
