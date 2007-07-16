include ${FSLCONFDIR}/default.mk

PROJNAME=sgeutils

SCRIPTS=fsl_sub
SYNONYMS=batch pbatch

DESTDIR=/usr/local/bin

INSTALL=install -p -c

all:

install:
	${INSTALL} -m 0775 fsl_sub ${FSLDEVDIR}/bin
	@for file in ${SYNONYMS} ; do \
		if [ -f $$file ] ; then \
			${LN} -m 0775 $$file ${DESTDIR} ; \
			echo ${LN} -m 0775 $$file ${DESTDIR} ; \
		fi \
	done
