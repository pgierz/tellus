#!/bin/bash
# Complete tellus-based workflow for Eem125-S2 ocean variable extraction

# 1. Ensure simulation exists and is configured
tellus simulation show Eem125-S2 || \
    tellus simulation create Eem125-S2 --model-id MPIOM

# 2. Associate location with proper path context
tellus simulation location add Eem125-S2 hsm.dmawi.de \
    --context '{"path_prefix": "/hs/D-P/projects/paleodyn/from_userc_pgierz/pgierz/cosmos-aso-wiso/Eem125-S2"}'

# 3. Register the split archive
tellus simulation archive add Eem125-S2 \
    --location hsm.dmawi.de \
    --pattern "Eem125-S2.tar.gz_*" \
    --split-parts 31 \
    --type split-tar

# 4. List archive contents (when implemented)
tellus simulation archive list-contents Eem125-S2 \
    --filter "*.grb" \
    --grep "mpiom" > grib_files.txt

# 5. Stage archives locally (when implemented)  
tellus simulation archive stage Eem125-S2 \
    --from-location hsm.dmawi.de \
    --to-location local-staging \
    --reconstruct

# 6. Extract variables (when implemented)
tellus simulation archive extract Eem125-S2 \
    --location local-staging \
    --variables THO,SAO \
    --output-format netcdf

# 7. Or run the complete workflow via tellus workflow (future)
tellus workflow create ocean-extraction \
    --from-template extract_ocean_variables \
    --simulation Eem125-S2

tellus workflow run start ocean-extraction \
    --engine snakemake \
    --parameters '{"variables": ["THO", "SAO"], "regrid_target": "r360x180"}'

# 8. Check status
tellus workflow run status ocean-extraction

# 9. List extracted files
tellus simulation files list Eem125-S2 --content-type output

# 10. Create intake catalog for easy data access
tellus simulation catalog create Eem125-S2 \
    --format intake \
    --output Eem125-S2_catalog.yaml