include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES = libcalc.la

CALC_LIB = calc

libcalc_la_SOURCES=  \
BoolInstruction.cpp  \
Calculator.cpp       \
ExtendedInstruction.cpp \
ExtendedInstructionTable.cpp \
JumpInstruction.cpp  \
RegisterReference.cpp \
CalcGrammar.ypp \
CalcLexer.lpp \
CalcAssembler.cpp \
SqlString.cpp \
ExtString.cpp

# noinst_PROGRAMS =    \
# testCalc tuple testCalcAssembler

# TESTS = $(noinst_PROGRAMS)

# testCalc_SOURCES=testCalc.cpp 
# tuple_SOURCES= tuple.cpp
# testCalcAssembler_SOURCES=testCalcAssembler.cpp

LDADD = -L$(top_builddir)/calc      \
		-l$(CALC_LIB)       \
	-L$(top_builddir)/libfennel \
		-lfennel_tuple      \
		-lfennel_common     \
	$(EXTRA_LDFLAGS)
