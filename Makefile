include ${FSLCONFDIR}/default.mk

PROJNAME=sgeutils

SCRIPTS=fsl_sub

DESTDIR=${FSLDEVDIR}/bin
CFGFILE=${FSLDEVDIR}/etc/fslconf/fsl_sub.cfg

INSTALL=install -p -c

all:

install:
	@for file in ${SCRIPTS} ; do \
		if [ -f $$file ] ; then \
			${INSTALL} -m 0775 $$file ${DESTDIR} ; \
			echo ${INSTALL} -m 0775 $$file ${DESTDIR} ; \
		fi \
	done
	${INSTALL} -m 0666 cfg ${CFGFILE}
