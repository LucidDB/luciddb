/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
    std::string traceFileName =
        configMap.getStringParam(paramTraceFileName,testName+"_trace.log");
    if (!traceFileName.empty()) {
        traceStream.open(traceFileName.c_str());
    }

    // NOTE: Sheesh.  Without this, Cygwin ostreams were failing after the
    // first attempt to output any numeric value because the referenced facet
    // was unintialized.  There must be a better way.  The number 14
    // comes from _Stl_loc_init_num_put in _num_put.h.
#ifdef __CYGWIN__
    _STL::num_put<
        char,
        _STL::ostreambuf_iterator<char,_STL::char_traits<char> > >::
        id._M_index = 14;
#endif
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
    // small (400K on my RedHat distribution) non-random sorted data set
    if (!configMap.isParamSet(paramDictionaryFileName)) {
#ifdef __MINGW32__
        std::string dictFileName = "d:\\cygwin\\usr\\share\\dict\\words";
#else
        std::string dictFileName = "/usr/share/dict/words";
#endif
        if (!FileSystem::doesFileExist(dictFileName.c_str())) {
            dictFileName = "/usr/dict/words";
        }
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
    if (traceStream.good()) {
        StrictMutexGuard traceMutexGuard(traceMutex);
        traceStream << "[" << source << "] " << message << std::endl;
        traceStream.flush();
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

// NOTE:  This pulls in all of the code for the Boost unit test framework.
// This way, it's only compiled and linked once into shared library
// libfenneltest, rather than linked statically into each unit test.  For a
// while, I was instead building the Boost unit test framework as a separate
// library via the normal Boost build.  This reduced Fennel build time, but
// didn't work on Cygwin.
#include <boost/test/included/unit_test_framework.hpp>

// End TestBase.cpp
