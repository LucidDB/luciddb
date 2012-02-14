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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/FileSystem.h"
#include "fennel/common/Backtrace.h"
#include "boost/test/test_tools.hpp"

using namespace fennel;
using boost::unit_test::test_unit;

ConfigMap TestBase::configMap;
bool TestBase::runAll = false;

ParamName TestBase::paramTestSuiteName = "testSuiteNameBoost";
ParamName TestBase::paramTraceFileName = "testTraceFileName";
ParamName TestBase::paramDictionaryFileName = "testDictionaryFileName";
ParamName TestBase::paramTraceLevel = "testTraceLevel";
ParamName TestBase::paramStatsFileName = "testStatsFileName";
ParamName TestBase::paramTraceStdout = "testTraceStdout";
ParamName TestBase::paramDegreeOfParallelism = "degreeOfParallelism";

TestBase::TestBase()
    : statsTarget(
        configMap.getStringParam(paramStatsFileName, "/tmp/fennel.stats")),
      statsTimer(statsTarget, 500),
      defaultTests(),
      extraTests()
{
    pTestObj.reset(this);
    testName = configMap.getStringParam(paramTestSuiteName);
    traceLevel = static_cast<TraceLevel>(
        configMap.getIntParam(paramTraceLevel, TRACE_CONFIG));
    std::string traceStdoutParam =
        configMap.getStringParam(paramTraceStdout, "");
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
/// With the option "-t TEST", runs only the test cases matching pattern TEST.
/// Note that -t functionality comes from Boost itself starting with v1.42.
/// CONFIGFILE is read to load configuration parameters.
/// Configuration parameters can also be set ad hoc, from the command line,
/// as pairs name=val. These take precedence.

void TestBase::readParams(int argc, char **argv)
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
            }
        } else {
            int i = arg.find("=");
            if ((0 < i) && (i < arg.size())) {
                // an ad hoc parameter
                std::string key = arg.substr(0, i);
                std::string val = arg.substr(i + 1);
                adhocMap.setStringParam(key, val);
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
        configMap.setStringParam(paramDictionaryFileName, dictFileName);
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

    TestSuite* pTestSuite = &(boost::unit_test::framework::master_test_suite());

    defaultTests.addAllToTestSuite(pTestSuite);
    if (runAll) {
        extraTests.addAllToTestSuite(pTestSuite);
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
    notifyTrace(testName, TRACE_INFO, "ENTER:  " + testCaseName);

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
    notifyTrace(testName, TRACE_INFO, "LEAVE:  " + testCaseName);
}

void TestBase::testCaseSetUp()
{
}

void TestBase::testCaseTearDown()
{
}

void TestBase::notifyTrace(std::string source, TraceLevel, std::string message)
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
#ifdef __MSVC__
    ::_sleep(nSeconds*1000);
#else
    ::sleep(nSeconds);
#endif
}

// End TestBase.cpp
