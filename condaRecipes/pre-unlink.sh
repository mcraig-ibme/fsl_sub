#!/bin/bash
if [ -n "${FSLDIR}" ]; then
    if [ -w "${FSLDIR}/bin" ]; then
        if [ -e "${FSLDIR}/etc/fslconf/requestFSLpythonLink.sh" ]; then
            "$FSLDIR/etc/fslconf/requestFSLpythonLink.sh" fsl_sub fsl_sub_config fsl_sub_plugin fsl_sub_report fsl_sub_update
        fi
    fi
fi
