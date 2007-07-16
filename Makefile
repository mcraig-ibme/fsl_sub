include ${FSLCONFDIR}/default.mk

PROJNAME=sgeutils

SCRIPTS=fsl_sub
SYNONYMS=batch pbatch

DESTDIR=/usr/local/bin

INSTALL=install -p -c
LN=ln -s

all:

install:
	@${MAKE} install-exec
	@${MAKE} install-links

install-exec:
	${INSTALL} -m 0775 fsl_sub ${FSLDEVDIR}/bin
	echo ${INSTALL} -m 0775 fsl_sub ${FSLDEVDIR}/bin

install-links:
	@for file in ${SYNONYMS} ; do \
		if [ -f $$file ] ; then \
			${LN} ${FSLDEVDIR}/bin/fsl_sub ${DESTDIR}/$$file ; \
			echo ${LN} ${FSLDEVDIR}/bin/fsl_sub ${DESTDIR}/$$file ; \
		fi \
	done
