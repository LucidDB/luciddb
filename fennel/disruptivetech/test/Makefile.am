include $(top_srcdir)/am_include.mk

if STATIC_TEST_LIBRARY
TEST_LIB=fenneltest
else
TEST_LIB=fenneltestdyn
endif

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
CollectExecStreamTest \
CalcAssemblerTest \
CalcInstFactoryTest \
CalcCastTest \
CalcMiscTest \
CalcExecStreamTest \
CalcExtRegExpTest \
CalcExtStringTest \
CalcExtContextTest \
CalcExtDateTimeTest \
CalcExtMathTest \
CalcExtCastTest \
CalcExtDynamicVariableTest \
SqlDateTest \
SqlRegExpTest \
SqlStringTest \
SqlStringAsciiTest \
CorrelationJoinExecStreamTest

TESTS = $(noinst_PROGRAMS)

CollectExecStreamTest_SOURCES=CollectExecStreamTest.cpp

CalcAssemblerTest_SOURCES=CalcAssemblerTest.cpp

CalcInstFactoryTest_SOURCES=CalcInstFactoryTest.cpp

CalcCastTest_SOURCES=CalcCastTest.cpp

CalcMiscTest_SOURCES=CalcMiscTest.cpp

CalcExecStreamTest_SOURCES=CalcExecStreamTest.cpp

CalcExtRegExpTest_SOURCES=CalcExtRegExpTest.cpp

CalcExtStringTest_SOURCES=CalcExtStringTest.cpp

CalcExtContextTest_SOURCES=CalcExtContextTest.cpp

CalcExtDateTimeTest_SOURCES=CalcExtDateTimeTest.cpp

CalcExtMathTest_SOURCES=CalcExtMathTest.cpp

CalcExtCastTest_SOURCES=CalcExtCastTest.cpp

CalcExtDynamicVariableTest_SOURCES=CalcExtDynamicVariableTest.cpp

SqlRegExpTest_SOURCES=SqlStringBuffer.cpp SqlRegExpTest.cpp 

SqlStringTest_SOURCES=SqlStringBuffer.cpp SqlStringTest.cpp 

SqlDateTest_SOURCES=SqlStringBuffer.cpp SqlDateTest.cpp 

SqlStringAsciiTest_SOURCES=SqlStringBuffer.cpp SqlStringAsciiTest.cpp 

CorrelationJoinExecStreamTest_SOURCES=CorrelationJoinExecStreamTest.cpp

# NOTE:  have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = \
	-L$(top_builddir)/test \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/disruptivetech/libfennel_dt \
	-l$(TEST_LIB) \
	-lfennel_dt_calc \
	-lfennel_dt_xo \
	-lfennel_xo \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common \
	-l$(STLPORT_LIB) \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	cp *.log .testlog/
