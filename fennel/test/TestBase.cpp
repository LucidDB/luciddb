/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifdef __CYGWIN__
#include <locale>
#endif

using namespace fennel;

ConfigMap TestBase::configMap;

ParamName TestBase::paramTestSuiteName = "testSuiteNameBoost";
ParamName TestBase::paramTraceFileName = "testTraceFileName";
ParamName TestBase::paramDictionaryFileName = "testDictionaryFileName";
ParamName TestBase::paramTraceLevel = "testTraceLevel";
ParamName TestBase::paramStatsFileName = "testStatsFileName";
ParamName TestBase::paramTraceStdout = "testTraceStdout";

TestBase::TestBase()
    : statsTarget(
        configMap.getStringParam(paramStatsFileName,"/tmp/fennel.stats")),
      statsTimer(statsTarget,500)
{
    pTestObj.reset(this);
    testName = configMap.getStringParam(paramTestSuiteName);
    traceLevel = static_cast<TraceLevel>(
        configMap.getIntParam(paramTraceLevel,TRACE_INFO));
    pTestSuite = BOOST_TEST_SUITE(testName.c_str());

    std::string traceStdoutParam = 
        configMap.getStringParam(paramTraceStdout,"");
    traceStdout = ((traceStdoutParam.length() == 0) ? false : true);

    std::string traceFileName =
        configMap.getStringParam(paramTraceFileName,testName+"_trace.log");

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
}

TestBase::~TestBase()
{
    traceStream.close();
    configMap.clear();
}

void TestBase::readParams(int argc,char **argv)
{
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        std::string configFileName = argv[i];
        if (configFileName == "-v") {
            verbose = true;
        } else if (configFileName == "-") {
            configMap.readParams(std::cin);
        } else {
            std::ifstream configFile(configFileName.c_str());
            assert(configFile.good());
            configMap.readParams(configFile);
        }
    }

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
    TestSuite *pRetTestSuite = pTestSuite;
    pTestSuite = NULL;
    return pRetTestSuite;
}

void TestBase::beforeTestCase(std::string testCaseName)
{
    notifyTrace(testName,TRACE_INFO,"ENTER:  " + testCaseName);
}

void TestBase::afterTestCase(std::string testCaseName)
{
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
