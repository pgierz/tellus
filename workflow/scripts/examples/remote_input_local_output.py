breakpoint()
print(f"{snakemake.input.hsm_file} --> {snakemake.output[0]}")
with open(snakemake.output[0], "w") as fo:
    with open(snakemake.input.hsm_file, "r") as fi:
        fo.write(fi.read())
