include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES = libcalc.la

CALC_LIB = calc

libcalc_la_SOURCES=  \
BoolInstruction.cpp  \
Calculator.cpp       \
ExtDateTime.cpp \
ExtendedInstruction.cpp \
ExtendedInstructionTable.cpp \
JumpInstruction.cpp  \
RegisterReference.cpp \
CalcGrammar.ypp \
CalcLexer.lpp \
CalcAssembler.cpp \
CalcInit.cpp \
SqlString.cpp \
SqlRegExp.cpp \
SqlStringAscii.cpp \
ExtString.cpp \
ExtRegExp.cpp \
StringToHex.cpp \
ExtMath.cpp

# noinst_PROGRAMS =    \
# testCalc tuple testCalcAssembler testCalcExtString

# TESTS = $(noinst_PROGRAMS)

# testCalc_SOURCES=testCalc.cpp 
# tuple_SOURCES= tuple.cpp
# testCalcAssembler_SOURCES=testCalcAssembler.cpp
# testCalcExtString_SOURCES=testCalcExtString.cpp

LDADD = -L$(top_builddir)/calc      \
		-l$(CALC_LIB)       \
	-L$(top_builddir)/libfennel \
		-lfennel_tuple      \
		-lfennel_common     \
	$(EXTRA_LDFLAGS)

DESIGN_DOC=ExtendedInstructionHowTo.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)

# Flex workaround:
# Explictly define how to generate a .cpp file from a flex .lpp file
# This is exactly what automake will do normally, 
# except we insert the line
# #include "fennel/common/CommonPreamble.h"
# at the beginning of the generated .cpp file.
# This makes sure that the headers are included in the correct
# order and compile flags such as _REENTRANT are not redefined
.lpp.cpp:
	$(LEXCOMPILE) `test -f $< || echo '$(srcdir)/'`$<
	echo "#include \"fennel/common/CommonPreamble.h\"" > $@
	sed '/^#/ s|$(LEX_OUTPUT_ROOT)\.c|$@|' $(LEX_OUTPUT_ROOT).c >> $@
	rm -f $(LEX_OUTPUT_ROOT).c
