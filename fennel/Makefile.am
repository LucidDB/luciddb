include $(top_srcdir)/am_include.mk

AUTOMAKE_OPTIONS=foreign

SUBDIRS=common synch device cache segment txn tuple exec btree \
db ftrs libfennel test @farrago_subdir@ disruptivetech lucidera build

EXTRA_DIST=am_include.mk moodss web test/verboseTest test/dictWords \
INSTALL.mingw

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

# In the distribution, add an extra level without a version number so
# that all include paths (internal and external) can be prefixed with
# fennel.
dist-hook:
	mkdir $(top_distdir)/../fennelStage
	mv $(distdir)/* $(top_distdir)/../fennelStage
	mv $(top_distdir)/../fennelStage $(distdir)/fennel

# Verify that all source files have the required headers
checkPreambles:
	build/checkPreambleFennel.sh

# Generate cscope index for all source files.  Depends on
# emacs_scope to produce cscope.files, but then reruns
# with a different set of flags.
cscope: emacs_cscope
	-rm cscope.out
	cscope -b -q -k

# Generate Emacs-compatible cscope index for all source files.
emacs_cscope:
	-rm cscope.*
	cscope-indexer -r
