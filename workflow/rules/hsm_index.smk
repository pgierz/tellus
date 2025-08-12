storage hsm:
  provider="fsspec",
  protocol="sftp",
  storage_options={"host": config['hsm_host'],},


def make_storage_argument(*args, **kwargs) -> str:
    print("args: ", args)
    print("kwargs: ", kwargs)
    print("config: ", config)
    for varname, varvalue in locals().items():
        print(f"{varname}: {varvalue}")
    breakpoint()
    return "who_knows"

rule create_remote_hsm_index:
  output:
      lambda params: make_storage_argument({wildcards.location_config})
      # storage.hsm("sftp://{{config['experiments'][{expid}]['locations'][{hsm_host}]['path']}}/{expid}_hsm_index.json")
      # storage.hsm(f"sftp://{config['experiments'][expid]['locations'][hsm_host]['hostname']}:22/hs/D-P/projects/paleodyn/simulations_pgierz/cosmos-aso-wiso/MIS11.3-B/MIS11.3-B_hsm_index.json")
      # storage.hsm("sftp://hsm.dmawi.de:22/hs/D-P/projects/paleodyn/simulations_pgierz/cosmos-aso-wiso/MIS11.3-B/MIS11.3-B_hsm_index.json")
  params:
      location_config=lambda wildcards: config['experiments'][wildcards.expid]['locations'][wildcards.hsm_host],
      # expid=wildcards.expid,
      script_path="workflow/scripts/create_hsm_index.py"
  shell:
      """
      echo "Creating remote HSM index for {wildcards.expid} on {wildcards.hsm_host}"
      echo "Output path: {output}"
      python {params.script_path} "{output}" "{json.dumps(config)}" "{json.dumps(wildcards)}"
      """

rule get_local_hsm_index:
  output:
      "archive/{hsm_host}/{expid}_hsm_index.json"
  input:
      hsm_file=lambda wildcards: storage.hsm(f"sftp://{wildcards.hsm_host}:22{config['experiments'][wildcards.expid]['locations'][wildcards.hsm_host]['path']}/{wildcards.expid}_hsm_index.json")
  params:
      username=lambda wildcards: config['experiments'][wildcards.expid]['locations'][wildcards.hsm_host]['storage_options']['username']
  shell:
      """
      echo "Fetching file via sftp from {input.hsm_file} with user {params.username}"
      echo "Output path: {output}"
      # Example command (not functional as-is):
      # sftp {params.username}@{wildcards.hsm_host}:{input.hsm_file} {output}
      """
  

