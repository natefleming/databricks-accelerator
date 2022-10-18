DIRS = lib
 
.PHONY: all dist clean distclean install uninstall help
 
all: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir all; \
	done

dist: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir dist; \
	done

clean: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir clean; \
	done

distclean: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir distclean; \
	done

install: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir install; \
	done

uninstall: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir uninstall; \
	done

help: 
	@for dir in $(DIRS); do \
		$(MAKE) -C $$dir help; \
	done

