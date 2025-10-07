#!/bin/bash -e 
# Comparison of various methods to see which one looks most understandable

EXPIDS="MIS11.3-B MIS17-B MIS19-B"

for EXPID in $EXPIDS; do
  sftp hsm.dmawi.de <<EOF
    ls -l /hs/D-P/projects/paleodyn/simulations_pgierz/cosmos-aso-wiso/$EXPID
    quit
EOF
done

for EXPID in $EXPIDS; do
  tellus simulation location ls -l "${EXPID}" tellus_hsm
done
