ifeq ($(wildcard Makefile),Makefile)
include Makefile
else
all $(filter-out configure Makefile distclean,$(MAKECMDGOALS)): Makefile
	$(MAKE) -f Makefile $(MAKECMDGOALS)
Makefile: configure
	./configure
configure:
	autoconf
distclean:
	@echo "configure wasn't run, aborting"
endif
