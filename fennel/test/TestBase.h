/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_TestBase_Included
#define Fennel_TestBase_Included

#include "fennel/common/ConfigMap.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/common/TraceTarget.h"
#include "fennel/common/FileStatsTarget.h"
#include "fennel/synch/StatsTimer.h"

#define BOOST_TEST_DYN_LINK

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <boost/test/parameterized_test.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <fstream>
#include <vector>

typedef boost::unit_test_framework::test_suite TestSuite;

FENNEL_BEGIN_NAMESPACE

/**
 * TestBase is the common base for all Fennel tests.
 */
class FENNEL_TEST_EXPORT TestBase
    : public TraceTarget,
        public boost::enable_shared_from_this<TestBase>

{
protected:
    /**
     * Boost test suite.
     */
    TestSuite *pTestSuite;

    boost::shared_ptr<TestBase> pTestObj;

    /**
     * Output file stream for tracing.
     */
    std::ofstream traceStream;

    /**
     * Protects traceStream.
     */
    StrictMutex traceMutex;

    /**
     * Name of test.
     */
    std::string testName;

    /**
     * Level at which to trace test execution.
     */
    TraceLevel traceLevel;

    /**
     * Output for stats.
     */
    FileStatsTarget statsTarget;

    /**
     * Timer for stats collection.
     */
    StatsTimer statsTimer;

    // TODO:  move to SynchObj
    void snooze(uint nSeconds);

    /**
     * Copy trace output to stdout
     */
    bool traceStdout;

    /**
     * Copy trace output to file
     */
    bool traceFile;

    /**
     * Run all test cases, including the extra tests.
     * (static, since set by readParams())
     */
    static bool runAll;

    /**
     * Collects a group of named test-case definitions.
     * Preserves the order; allows lookup by name.
     */
    class FENNEL_TEST_EXPORT TestCaseGroup
    {
        struct Item
        {
            std::string name;
            boost::unit_test::test_unit * tu;
            Item(std::string name, boost::unit_test::test_unit* tu)
                : name(name), tu(tu) {}
        };
        /** the test cases, in order of definition */
        std::vector<Item> items;
    public:
        void addTest(std::string name, boost::unit_test::test_unit *tu);
        boost::unit_test::test_unit* findTest(std::string name) const;
        void addAllToTestSuite(TestSuite *) const;
    };

    TestCaseGroup defaultTests;
    TestCaseGroup extraTests;

public:
    static ParamName paramTestSuiteName;
    static ParamName paramTraceFileName;
    static ParamName paramDictionaryFileName;
    static ParamName paramTraceLevel;
    static ParamName paramStatsFileName;
    static ParamName paramTraceStdout;
    static ParamName paramDegreeOfParallelism;

    /**
     * Configuration parameters.  The reason this is static is so that no
     * constructor parameters (which burden virtual bases) are needed.
     */
    static ConfigMap configMap;

    explicit TestBase();
    virtual ~TestBase();

    // helpers for FENNEL_UNIT_TEST_SUITE etc. below
    static void readParams(int argc, char **argv);
    TestSuite *releaseTestSuite();
    void beforeTestCase(std::string testCaseName);
    void afterTestCase(std::string testCaseName);

    /**
     * Equivalent to JUnit TestCase.setUp; this is called before each test case
     * method is invoked.  Default is no-op.
     */
    virtual void testCaseSetUp();

    /**
     * Equivalent to JUnit TestCase.tearDown; this is called after each test
     * case method is invoked.  Default is no-op.
     */
    virtual void testCaseTearDown();

    // implement TraceTarget
    virtual void notifyTrace(
        std::string source, TraceLevel level, std::string message);
    virtual TraceLevel getSourceTraceLevel(std::string source);
};

/**
 * TestWrapperTemplate wraps a test class with setUp/tearDown hooks around
 * each test case method invocation.
 */
template<class UserTestClass>
class TestWrapperTemplate
{
public:
    typedef void (UserTestClass::*FunctionType)();

private:
    std::string name;
    boost::shared_ptr<UserTestClass> pUserTestCase;

public:
    // Constructor
    TestWrapperTemplate(
        std::string nameInit,
        boost::shared_ptr<UserTestClass>& pUserTestCaseInit)
        : pUserTestCase(pUserTestCaseInit)
    {
        name = nameInit;
    }

    void runTest(FunctionType pFunction)
    {
        pUserTestCase->beforeTestCase(name);
        pUserTestCase->testCaseSetUp();
        try {
            try {
                ((*pUserTestCase).*pFunction)();
            } catch (std::exception &ex) {
                pUserTestCase->notifyTrace(name, TRACE_SEVERE, ex.what());
                throw ex;
            }
        } catch (...) {
            try {
                pUserTestCase->testCaseTearDown();
            } catch (...) {
                // ignore teardown errors after failure
            }
            throw;
        }
        try {
            pUserTestCase->testCaseTearDown();
        } catch (...) {
            pUserTestCase->afterTestCase(name);
            throw;
        }
        pUserTestCase->afterTestCase(name);
    }
};

// FENNEL_UNIT_TEST_SUITE should be invoked at top scope within the .cpp file
// defining a test class derived from TestBase, with UserTestClass equal to the
// derived class name.

#define FENNEL_UNIT_TEST_SUITE(UserTestClass) \
bool init_unit_test() \
{ \
    TestBase::readParams( \
        boost::unit_test::framework::master_test_suite().argc, \
        boost::unit_test::framework::master_test_suite().argv); \
    std::string paramKey(TestBase::paramTestSuiteName); \
    std::string paramVal(#UserTestClass); \
    TestBase::configMap.setStringParam(paramKey, paramVal); \
    UserTestClass *pTestObj = new UserTestClass(); \
    TestBase::configMap.disableTracing(); \
    pTestObj->releaseTestSuite(); \
    return true; \
} \
\
int main(int argc, char **argv) \
{ \
    return ::boost::unit_test::unit_test_main(&init_unit_test, argc, argv); \
}



// In the test class constructor, invoke either FENNEL_UNIT_TEST_CASE or
// FENNEL_EXTRA_UNIT_TEST_CASE for each test case method. Call
// FENNEL_UNIT_TEST_CASE to define a test case that is run by default. Call
// FENNEL_EXTRA_UNIT_TEST_CASE to define an extra test case that is run only
// when selected from the command line, either by "-t TESTNAME" or by "-all".

#define FENNEL_UNIT_TEST_CASE(UserTestClass, testMethodName) \
  FENNEL_DEFINE_UNIT_TEST_CASE(defaultTests, UserTestClass, testMethodName)

#define FENNEL_EXTRA_UNIT_TEST_CASE(UserTestClass, testMethodName) \
  FENNEL_DEFINE_UNIT_TEST_CASE(extraTests, UserTestClass, testMethodName)

// This macro is based on BOOST_PARAM_CLASS_TEST_CASE():
// make_test_case() below actually returns a test_unit_generator, not a
// test_case. The generator emits one test.
#define FENNEL_DEFINE_UNIT_TEST_CASE(group, UserTestClass, testMethodName) \
do { \
    typedef TestWrapperTemplate<UserTestClass> TestWrapper; \
    boost::shared_ptr<UserTestClass> pDerivedTestObj = \
        boost::dynamic_pointer_cast<UserTestClass>(pTestObj); \
    TestWrapper::FunctionType params [] = \
        { &UserTestClass::testMethodName }; \
    boost::unit_test::test_unit *tu = \
        boost::unit_test::make_test_case<TestWrapper>(\
            &TestWrapper::runTest, \
            #testMethodName, \
            boost::shared_ptr<TestWrapper>(new TestWrapper(\
                #testMethodName, \
                pDerivedTestObj)), \
            params, \
            params + 1).next(); \
    group.addTest(#testMethodName, tu); \
} while (0)

FENNEL_END_NAMESPACE

#endif

// End TestBase.h
