SCRIPTS=batch pbatch fsl_sub fsl_sub_shepherd

DESTDIR=/usr/local/bin
CFGDIR=/usr/local/share/fsl_sub

INSTALL=install -p -c

all:

install:
	@for file in ${SCRIPTS} ; do \
		if [ -f $$file ] ; then \
			${INSTALL} -m 0775 $$file ${DESTDIR} ; \
			echo ${INSTALL} -m 0775 $$file ${DESTDIR} ; \
		fi \
	done
	${INSTALL} -m 0666 cfg ${CFGDIR}
