SCRIPTS=batch pbatch

DESTDIR=/usr/local/bin

INSTALL=install -p -c

install:
	@for file in ${SCRIPTS} ; do \
		if [ -f $$file ] ; then \
			${INSTALL} -m 0775 $$file ${DESTDIR} ; \
			echo ${INSTALL} -m 0775 $$file ${DESTDIR} ; \
		fi \
	done

all:
