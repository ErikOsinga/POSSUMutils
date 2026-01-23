"""
This job manually refreshes the cadcproxy.pem in ~/.ssl directory with the prefect secret cadc-proxy-pem.
This is in case the cadcproxy.pem needs to be rewritten in case there was a mistake in the secret.
Otherwise the file is only reloaded from the secret if the file has expired (more then 30 days old).
"""
from prefect import flow
from prefect.blocks.system import Secret
from possum_pipeline_control import util

@flow(log_prints=True)
def main():
    # Load the secret from prefect
    print('Loading cadc-proxy-pem secret..')
    secret_block = Secret.load("cadc-proxy-pem")
    pem_str = secret_block.get()
    if pem_str:
        #rewrite the pem file
        print('Rewriting cadcproxy.pem file from the secret..')
        util.write_cadcproxy_pem(pem_str)
        print('Finished rewriting cadcproxy.pem')
    else:
        raise FileNotFoundError(
            f"Required CADC certificate not found in ~/.ssl. {util.update_cadc_proxy_msg}"                
        )