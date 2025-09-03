# Ocean Variable Extraction Workflow

This example workflow demonstrates how to extract ocean variables (THO and SAO) from split MPIOM archives using tellus and Snakemake.

## Overview

This workflow:
1. Stages split archive parts from remote storage (e.g., HSM tape)
2. Reconstructs the complete archive from split parts
3. Extracts GRIB files containing ocean data
4. Converts GRIB to NetCDF with proper variable names
5. Regrids data to regular lat-lon grid (r360x180)
6. Organizes output following standard directory structure

## Prerequisites

- `tellus` configured with access to data location
- `cdo` (Climate Data Operators) with MPIOM variable table support
- `ncatted` (from NCO tools)
- `snakemake`

## Directory Structure

```
Eem125-S2/
├── outdata/
│   └── mpiom/
│       └── Eem125-S2_mpiom_24900101_24901231.grb  # Original GRIB files
└── analysis/
    └── mpiom/
        ├── Eem125-S2_mpiom_THO_24900101_24901231_r360x180.nc  # Extracted & regridded
        └── Eem125-S2_mpiom_SAO_24900101_24901231_r360x180.nc
```

## Configuration

Edit `config.yaml` to set:
- `simulation.id`: Simulation identifier
- `archive.location`: Remote storage location
- `archive.num_parts`: Number of split archive parts
- `paths.base_dir`: Where to create output structure
- `variables`: List of variables to extract
- `timestamps`: Time periods to process

## Usage

### Basic Run
```bash
snakemake --cores 4
```

### With Custom Config
```bash
snakemake --configfile my_config.yaml --cores 4
```

### Dry Run (see what would be executed)
```bash
snakemake -n
```

### Force Rerun
```bash
snakemake --forceall --cores 4
```

## Workflow Steps

1. **Stage Archive Parts**: Downloads all split parts using `tellus simulation location mget`
2. **Reconstruct Archive**: Concatenates parts into single tar.gz
3. **Extract GRIB Files**: Extracts with prefix stripping to handle internal paths
4. **Extract Variables**: Uses CDO with MPIOM variable table (`-t mpiom1`)
5. **Regrid**: Interpolates to regular r360x180 grid
6. **Generate Report**: Creates summary of extracted files

## Troubleshooting

### Archive Path Issues
If extraction fails due to internal paths, adjust `archive.internal_prefix` and `extraction.strip_components` in config.

### Variable Not Found
Ensure the variable name matches MPIOM conventions. Check available variables:
```bash
cdo -t mpiom1 showname input.grb
```

### Memory Issues
For large files, you may need to process timesteps individually or increase available memory.

## Example: Eem125-S2

The provided config demonstrates extraction of ocean temperature (THO) and salinity (SAO) from the Eem125-S2 simulation, with 31 split archive parts totaling ~60GB.