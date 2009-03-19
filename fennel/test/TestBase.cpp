/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/FileSystem.h"
#include "fennel/common/Backtrace.h"
#include "boost/test/test_tools.hpp"

#ifdef __CYGWIN__
#include <locale>
#endif

using namespace fennel;
using boost::unit_test::test_unit;

ConfigMap TestBase::configMap;
bool TestBase::runAll = false;
std::string TestBase::runSingle;

ParamName TestBase::paramTestSuiteName = "testSuiteNameBoost";
ParamName TestBase::paramTraceFileName = "testTraceFileName";
ParamName TestBase::paramDictionaryFileName = "testDictionaryFileName";
ParamName TestBase::paramTraceLevel = "testTraceLevel";
ParamName TestBase::paramStatsFileName = "testStatsFileName";
ParamName TestBase::paramTraceStdout = "testTraceStdout";
ParamName TestBase::paramDegreeOfParallelism = "degreeOfParallelism";

TestBase::TestBase()
    : statsTarget(
        configMap.getStringParam(paramStatsFileName,"/tmp/fennel.stats")),
      statsTimer(statsTarget,500)
{
    pTestObj.reset(this);
    testName = configMap.getStringParam(paramTestSuiteName);
    traceLevel = static_cast<TraceLevel>(
        configMap.getIntParam(paramTraceLevel,TRACE_CONFIG));
    std::string traceStdoutParam =
        configMap.getStringParam(paramTraceStdout,"");
    traceStdout = ((traceStdoutParam.length() == 0) ? false : true);

    std::string defaultTraceFileName;
    const char *fennelHome = getenv("FENNEL_HOME");
    if (fennelHome) {
        defaultTraceFileName += fennelHome;
        defaultTraceFileName += "/trace/";
    }
    defaultTraceFileName += testName + "_trace.log";
    std::string traceFileName =
        configMap.getStringParam(
            paramTraceFileName,
            defaultTraceFileName);

    traceFile = false;
    if (traceFileName == "-") {
        traceStdout = true;
    } else if (traceFileName != "none") {
        if (!traceFileName.empty()) {
            traceStream.open(traceFileName.c_str());
            if (traceStream.good()) {
                traceFile = true;
            }
        }
    }

    // NOTE jvs 25-Nov-2008:  This is to make sure we trace any
    // configuration access in the derived class constructor.
    // There's a matching call to disableTracing in
    // the definition for FENNEL_UNIT_TEST_SUITE after
    // the constructor returns.
    configMap.initTraceSource(shared_from_this(), "testConfig");
}

TestBase::~TestBase()
{
    traceStream.close();
    configMap.clear();
}


/// Parses the command line.
/// format: [-v] [-t TEST | -all] {param=val}* [CONFIGFILE | -]
/// Normally, the test program runs the default test cases.
/// With the option "-all", runs the extra test cases as well.
/// With the option "-t TEST", runs only the single test case named TEST.
/// CONFIGFILE is read to load configuration parameters.
/// Configuration parameters can also be set ad hoc, from the command line,
/// as pairs name=val. These take precedence.

void TestBase::readParams(int argc,char **argv)
{
    bool verbose = false;
    ConfigMap adhocMap;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (argv[i][0] == '-') {
            if (arg == "-v") {
                verbose = true;
            } else if (arg == "-") {
                configMap.readParams(std::cin);
            } else if (arg == "-all") {
                runAll = true;
            } else if (arg == "-t") {   // -t TEST
                permAssert(i + 1 < argc);
                runSingle = argv[++i];
            } else if (arg[1] == 't') { // allow -tTEST
                runSingle = arg.substr(2);
            }
        } else {
            int i = arg.find("=");
            if ((0 < i) && (i < arg.size())) {
                // an ad hoc parameter
                std::string key = arg.substr(0,i);
                std::string val = arg.substr(i + 1);
                adhocMap.setStringParam(key,val);
            } else {
                // a config file name
                std::ifstream configFile(arg.c_str());
                assert(configFile.good());
                configMap.readParams(configFile);
            }
        }
    }
    configMap.mergeFrom(adhocMap);

    // set a default dictionary file location for use by tests that need a
    // small non-random sorted data set
    if (!configMap.isParamSet(paramDictionaryFileName)) {
        std::string dictFileName = "dictWords";
        configMap.setStringParam(paramDictionaryFileName,dictFileName);
    }

    if (verbose) {
        configMap.dumpParams(std::cout);
    }
}

TestSuite *TestBase::releaseTestSuite()
{
    assert(pTestObj);
    assert(pTestObj.use_count() > 1);

    // release self-reference now that all test cases have been registered
    pTestObj.reset();

    TestSuite* pTestSuite = BOOST_TEST_SUITE(testName.c_str());

    if (runSingle.size()) {
        test_unit *p =  defaultTests.findTest(runSingle);
        if (!p) {
            p = extraTests.findTest(runSingle);
        }
        if (!p) {
            std::cerr << "test " << runSingle << " not found\n";
            exit(2);
        }
        pTestSuite->add(p);
    } else {
        defaultTests.addAllToTestSuite(pTestSuite);
        if (runAll) {
            extraTests.addAllToTestSuite(pTestSuite);
        }
    }
    return pTestSuite;
}

void TestBase::TestCaseGroup::addTest(std::string name, test_unit *tu)
{
    items.push_back(Item(name, tu));
}

test_unit*
TestBase::TestCaseGroup::findTest(std::string name) const
{
    for (std::vector<Item>::const_iterator p = items.begin();
         p != items.end(); ++p)
    {
        if (name == p->name) {
            return p->tu;
        }
    }
    return 0;
}

void TestBase::TestCaseGroup::addAllToTestSuite(TestSuite *suite) const
{
    for (std::vector<Item>::const_iterator p = items.begin();
         p != items.end(); ++p)
    {
        suite->add(p->tu);
    }
}


void TestBase::beforeTestCase(std::string testCaseName)
{
    notifyTrace(testName,TRACE_INFO,"ENTER:  " + testCaseName);

    // Install the AutoBacktrace signal handler now, after
    // boost::execution_monitor::catch_signals() has installed its own, so that
    // on SIGABRT AutoBacktrace goes first, prints the backtrace, then chains
    // to boost, which handles the error.
    AutoBacktrace::setOutputStream();
    AutoBacktrace::setTraceTarget(shared_from_this());
    AutoBacktrace::install();
    configMap.initTraceSource(shared_from_this(), "testConfig");
}

void TestBase::afterTestCase(std::string testCaseName)
{
    AutoBacktrace::setTraceTarget();
    configMap.disableTracing();
    notifyTrace(testName,TRACE_INFO,"LEAVE:  " + testCaseName);
}

void TestBase::testCaseSetUp()
{
}

void TestBase::testCaseTearDown()
{
}

void TestBase::notifyTrace(std::string source,TraceLevel,std::string message)
{
    if (traceFile || traceStdout) {
        StrictMutexGuard traceMutexGuard(traceMutex);
        if (traceFile) {
            traceStream << "[" << source << "] " << message << std::endl;
            traceStream.flush();
        }
        if (traceStdout) {
            std::cout << "[" << source << "] " << message << std::endl;
            std::cout.flush();
        }
    }
}

TraceLevel TestBase::getSourceTraceLevel(std::string)
{
    return traceLevel;
}

void TestBase::snooze(uint nSeconds)
{
#ifdef __MINGW32__
    ::_sleep(nSeconds*1000);
#else
    ::sleep(nSeconds);
#endif
}

#ifdef __MINGW32__
// REVIEW jvs 31-Aug-2005:  I had to add this to shut up the linker
// when moving to boost 1.33.  According to the boost docs, the
// linker's complaint indicates the potential for a leak due to
// the interaction between threads and DLL's.  Since it's only
// for tests, figuring out what's going wrong is low priority.
extern "C" void tss_cleanup_implemented(void)
{
}
#endif

// NOTE:  This pulls in all of the code for the Boost unit test framework.
// This way, it's only compiled and linked once into shared library
// libfenneltest, rather than linked statically into each unit test.  For a
// while, I was instead building the Boost unit test framework as a separate
// library via the normal Boost build.  This reduced Fennel build time, but
// didn't work on Cygwin.
#include <boost/test/included/unit_test_framework.hpp>

// End TestBase.cpp
