include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES = libcalculator.la

CALCULATOR_LIB = calculator

libcalculator_la_SOURCES=  \
BoolInstruction.cpp  \
CalcExecStream.cpp       \
CalcExcn.cpp       \
Calculator.cpp       \
ExtDateTime.cpp \
ExtendedInstruction.cpp \
ExtendedInstructionTable.cpp \
JumpInstruction.cpp  \
RegisterReference.cpp \
CalcGrammar.ypp \
CalcLexer.lpp \
CalcAssembler.cpp \
InstructionFactory.cpp \
InstructionSignature.cpp \
CalcInit.cpp \
SqlDate.cpp \
SqlString.cpp \
SqlRegExp.cpp \
SqlStringAscii.cpp \
ExtString.cpp \
ExtCast.cpp \
ExtRegExp.cpp \
StringToHex.cpp \
ExtMath.cpp \
ExtDynamicVariable.cpp \
WinAggHistogramStrA.cpp \
ExtWinAggFuncs.cpp \
NoisyArithmetic.cpp

DESIGN_DOC=ExtendedInstructionHowTo.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC) $(DISABLED_TESTS)
ETAGS_ARGS=$(DESIGN_DOC) $(DISABLED_TESTS)

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
