/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/common/TraceSource.h"

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/disruptivetech/calc/StringToHex.h"
#include "fennel/disruptivetech/calc/InstructionFactory.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"
#include "fennel/disruptivetech/calc/ExtendedInstruction.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;


class CalcExtContextTest : virtual public TestBase, public TraceSource
{
    void setupExtendedTestInstructions();
    void testCalcExtContext();
    void testCalcExtContextPost();

    void printOutput(TupleData const & tup,
                     Calculator const & calc);

public:
    explicit CalcExtContextTest()
        : TraceSource(shared_from_this(),"CalcExtContextTest")
    {
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtContextTest, testCalcExtContext);
        FENNEL_UNIT_TEST_CASE(CalcExtContextTest, testCalcExtContextPost);
    }

    virtual ~CalcExtContextTest()
    {
    }
};

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtContextTest::printOutput(TupleData const & tup,
                                Calculator const & calc)
{
#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
    cout << endl;
#endif
}

// Simple context class for testing purposes. Not thread safe.
class EICtx : public ExtendedInstructionContext
{
public:
    EICtx()
    {
        mCount++;
        BOOST_MESSAGE(mCount);
    }
    ~EICtx()
    {
        mCount--;
        BOOST_MESSAGE(mCount);
    }
    static int mCount;
};

int EICtx::mCount = 0;

void
ctxInst1(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst2(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst3(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst4(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3,
         RegisterRef<bool>* dummy4)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst5(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3,
         RegisterRef<bool>* dummy4,
         RegisterRef<bool>* dummy5)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
CalcExtContextTest::setupExtendedTestInstructions()
{
    ExtendedInstructionTable* eit = InstructionFactory::getExtendedInstructionTable();
    ExtendedInstructionDef* inst;

    vector<StandardTypeDescriptorOrdinal>params;
    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst1", params,
             (ExtendedInstruction1Context<bool>*) NULL,
             ctxInst1);
    inst = (*eit)["ctxInst1(bo)"];
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst1"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 1);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst2", params,
             (ExtendedInstruction2Context<bool,bool>*) NULL,
             ctxInst2);
    inst = (*eit)["ctxInst2(bo,bo)"];
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst2"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 2);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst3", params,
             (ExtendedInstruction3Context<bool,bool,bool>*) NULL,
             ctxInst3);
    inst = (*eit)["ctxInst3(bo,bo,bo)"];
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst3"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 3);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst4", params,
             (ExtendedInstruction4Context<bool,bool,bool,bool>*) NULL,
             ctxInst4);
    inst = (*eit)["ctxInst4(bo,bo,bo,bo)"];
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst4"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 4);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst5", params,
             (ExtendedInstruction5Context<bool,bool,bool,bool,bool>*) NULL,
             ctxInst5);
    inst = (*eit)["ctxInst5(bo,bo,bo,bo,bo)"];
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst5"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 5);

}


void
CalcExtContextTest::testCalcExtContext()
{
    // add in some extended instructions after init
    setupExtendedTestInstructions();

    ostringstream pg("");

    pg << "O bo,bo,bo,bo,bo,bo;" << endl;
    pg << "L bo,bo,bo,bo,bo,bo;" << endl;
    pg << "C bo;" << endl;
    pg << "V 0;" << endl;
    pg << "T;" << endl;
    pg << "MOVE L1, C0;" << endl;
    pg << "MOVE L2, C0;" << endl;
    pg << "MOVE L3, C0;" << endl;
    pg << "MOVE L4, C0;" << endl;
    pg << "MOVE L5, C0;" << endl;
    pg << "CALL 'ctxInst1(L1);" << endl;
    pg << "CALL 'ctxInst2(L2,L0);" << endl;
    pg << "CALL 'ctxInst3(L3,L0,L0);" << endl;
    pg << "CALL 'ctxInst4(L4,L0,L0,L0);" << endl;
    pg << "CALL 'ctxInst5(L5,L0,L0,L0,L0);" << endl;
    pg << "REF O0, L0;" << endl;
    pg << "REF O1, L1;" << endl;
    pg << "REF O2, L2;" << endl;
    pg << "REF O3, L3;" << endl;
    pg << "REF O4, L4;" << endl;
    pg << "REF O5, L5;" << endl;

    //    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    int i;
    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          true);
    }


    // call program again, should get different output
    calc.exec();

    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          false);
    }

    // call program again, should get same output
    calc.exec();

    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          false);
    }


#if 0
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;
#endif

}

// be sure that all context objects were destroyed
void
CalcExtContextTest::testCalcExtContextPost()
{
    BOOST_CHECK_EQUAL(EICtx::mCount, 0);
}


FENNEL_UNIT_TEST_SUITE(CalcExtContextTest);

// End CalcExtContextTest.cpp
