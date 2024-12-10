#!python

#
# vdbfs.py
# version 0.3
#
def usage():
    usage=f"""
    Your args: {" ".join(sys.argv)}

Overview:
    Command Line Interface into Databricks Volumes with Lineage Capture.

    This is a prototype python script for downloading and uploading files from/to well 
    governed Unity Catalog using open tools (e.g. AWS cli) that run at the maximum cloud 
    based performance. A direct connection to your S3 bucket.
    
    This script generates temporary credentials and then passes those credentials onto
    the AWS cli. Credentials are only vended if the user has 'READ_VOLUME' (or 'WRITE_VOLUME')
    
Pre-requisites:
    pip install awscli==1.36.17 boto3==1.35.76 databricks-sdk==0.38.0
    Permissions on Volume:
        GRANT READ VOLUME ON VOLUME catalog.schema.volume_name TO `first.lastname@example.com` or
        GRANT WRITE VOLUME ON VOLUME catalog.schema.volume_name TO `first.lastname@example.com`

Usage:
    databricks auth login --profile <your databricks profile>
    export HOST=<databricks workspace url>
    python {sys.argv[0]} ls dbfs:/Volumes/singlecell/cellxgene/raw_hd5
    python {sys.argv[0]} cp /Volumes/singlecell/cellxgene/raw_hd5/csvs/21d3e683-80a4-4d9b-bc89-ebb2df513dde_obs.csv x.csv
    python {sys.argv[0]} creds dbfs:/Volumes/singlecell/cellxgene/raw_hd5
    python {sys.argv[0]} put ./x.csv dbfs:/Volumes/singlecell/cellxgene/processed_h5ad/x.csv
"""
    return usage

def api_client_do(w, method:str, path:str, data:dict):
    #print(data)
    return w.api_client.do(
            method=method,
            path=path,
            headers={"Content-Type": "application/json"},
            body=data,
        )

TEMP_VOLUME_CREDENTIALS_PATH="/api/2.0/unity-catalog/temporary-volume-credentials"
LINEAGE_CUSTOM_PATH="/api/2.0/lineage-tracking/custom/"
def byol_create(data): return api_client_do(w, "POST", LINEAGE_CUSTOM_PATH, data)
def byol_update(data): return api_client_do(w, "PATCH", LINEAGE_CUSTOM_PATH, data)
def byol_delete(data): return api_client_do(w, "DELETE", LINEAGE_CUSTOM_PATH, data)
def byol_list(data):   return api_client_do(w, "GET", LINEAGE_CUSTOM_PATH, data)
def temp_volume_credentials_get(w, volume_id, operation):
    return api_client_do(
        w, 
        method="POST", 
        path=TEMP_VOLUME_CREDENTIALS_PATH, 
        data={
            "volume_id": volume_id,
            "operation": operation
        }
    )

def vol_parts(source_path:str, url=''):
    parts = source_path.split('/')
    catalog = parts[2]
    schema = parts[3]
    volume = parts[4]
    relative_path = '/'.join(parts[5:])
    return f"{catalog}.{schema}.{volume}", f"{url}/{relative_path}"

def get_creds(w, volume_full_name:str, operation="READ_VOLUME"):
    """
        Given Volume Name, return temporary (AWS) credentials and s3 url.
        User must have access to the volume within Unity Catalog.

    Args:
        w (WorkspaceClient): Databricks workspace connection object.
        volume_full_name (str): e.g. dbfs:/Volumes/catalog/schema/volume/path/file.ext
        operation (str, optional):  Defaults to "READ_VOLUME". Override to "WRITE_VOLUME"

    Returns:
        Tuple: temporary credentials and s3 url to volume root.
    """
    v = w.volumes.read(volume_full_name)
    response = temp_volume_credentials_get(w, v.volume_id, operation)  
    return response['aws_temp_credentials'], response['url']

def do_show_creds(w, source_path:str, operation="READ_VOLUME"):
    volume_full_name,rel_path = vol_parts(source_path)
    creds,url = get_creds(w, volume_full_name=volume_full_name, operation=operation)
    print(f"""
export AWS_ACCESS_KEY_ID="{creds['access_key_id']}"
export AWS_SECRET_ACCESS_KEY="{creds['secret_access_key']}"
export AWS_SESSION_TOKEN="{creds['session_token']}"

aws s3 ls --recursive {url}
        """)


def do_command(w, command:str, source_path:str, destination:str, host:str):
    """
    Connect to Databricks, get temporary credentials, then setup aws cli command and run it.

    Args:
        w (WorkspaceClient): Databricks workspace connection object.
        command (str): Short command like 'ls' or 'cp'
        source_path (str): Full databricks dbfs path to volume and file 'dbfs:/Volumes/catalog/schema/volume/path/file'
        destination (str): Local filesystem destination
        host (str): Databricks host url (without trailing /)
    """
    assert w is not None
    assert command is not None

    try:
        direction = "download" if "/Volumes" in source_path else "upload"
        operation = 'WRITE_VOLUME' if direction == "upload" else 'READ_VOLUME'

        if command == 'ls':
            volume_full_name,rel_path = vol_parts(source_path)
            creds,url = get_creds(w, volume_full_name=volume_full_name, operation=operation)
            s3_url = f"{url}{rel_path}"
            shell_command = f"aws s3 {command} {s3_url}"
        if command == 'cp':
            volume_full_name,rel_path = vol_parts(source_path)
            creds,url = get_creds(w, volume_full_name=volume_full_name, operation=operation)
            s3_url = f"{url}{rel_path}"
            shell_command = f"aws s3 {command} {s3_url} {destination}"
        if command == 'creds':
            shell_command = None
            operation = 'WRITE_VOLUME'
            do_show_creds(w, source_path, operation=operation)

        if command == 'put':
            volume_full_name,rel_path = vol_parts(destination)
            creds,url = get_creds(w, volume_full_name=volume_full_name, operation=operation)
            s3_url = f"{url}{rel_path}"
            shell_command = f"aws s3 cp .{rel_path} {s3_url}"
        
        if shell_command:
            print(f"\n\nRunning command \n{shell_command}\n...")
            # stuff temp credentials into environment to be picked up by aws cli subprocess
            os.environ["AWS_ACCESS_KEY_ID"]=creds['access_key_id']
            os.environ["AWS_SECRET_ACCESS_KEY"]=creds['secret_access_key']
            os.environ["AWS_SESSION_TOKEN"]=creds['session_token']
            results = subprocess.run(shell_command.split())
            
    except Exception as e:
        logging.error(f"do_command {command} failed: {e}")

def localhost():
    return socket.gethostname()

def url(file_name):
    return f"file://{w.current_user.me().user_name}@{localhost()}/{os.getcwd()}/{file_name}"

def client_guid():
    return f"download/{uuid.uuid4()}"

def do_lineage(w, cguid, source_path, destination_path):
    direction = "download" if "/Volumes" in source_path else "upload"
    return byol_create(data={
        "entities": [
            {
            "entity_id": {
                "provider_type": "CUSTOM",
                "guid": cguid
            },
            "entity_type": f"file/{direction}",
            "display_name": f"{destination_path}",
            "url": url(destination_path),
            "description": "downloads",
            "properties": json.dumps({
                "source_path": source_path,
                "destination_path": destination_path,
                "destination_host": localhost(),
                "destination_cwd":  os.getcwd()
            })
            }
        ],
        "relationships": [
            {
                "source": {
                    "provider_type": "DATABRICKS",
                    "databricks_type": "PATH",
                    "guid": "dbfs:/Volumes/singlecell/cellxgene/raw_hd5"
                },
                "target": {
                    "provider_type": "CUSTOM",
                    "guid": cguid
                }
            }
        ]
        })


def handle_inputs():
    # handle inputs
    host = os.environ.get("DATABRICKS_HOST",None)
    if not host:
        print(f"Error: DATABRICKS_HOST is not set\n\n {usage()}")
        exit(2)

    command = source_path = destination = None
    if len(sys.argv) >= 2:
        command = sys.argv[1]
    
    if command == 'ls':
        source_path = sys.argv[2]
        assert source_path is not None
    elif command == 'cp':
        source_path = sys.argv[2]
        destination = sys.argv[3]
        assert destination is not None
    elif command == 'creds':
        source_path = sys.argv[2]
        assert source_path is not None
    elif command == 'put':
        source_path = sys.argv[2]
        destination = sys.argv[3]
        assert source_path is not None
        assert destination is not None
    else:
        logging.info(f"Exiting on usage {host} {command} {source_path} {destination}")
        print(usage())
        exit(1)

    logging.info(f"{host}, {command}, {source_path}, {destination}")
    return host, command, source_path, destination

## ---------
if __name__ == "__main__":
    import os
    import sys
    import socket
    from datetime import datetime
    import subprocess
    import logging
    import json
    import uuid

    logging.basicConfig(stream=sys.stderr,
                        level=logging.INFO,
                        format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
    logging.getLogger('databricks.sdk').setLevel(logging.DEBUG)
    # set loglevel to DEBUG for debugging

    host, command, source_path, destination = handle_inputs()
    
    #display usage before non-default imports
    try:
        from databricks.sdk import WorkspaceClient
        import boto3
    except ModuleNotFoundError as e:
        logging.error(f"failed to import: {e}")
        logging.info(usage())
        exit(3)

    try:
        # do command and record lineage
        w=WorkspaceClient(host=host)
        do_command(w, command=command, source_path=source_path, destination=destination, host=host)
        if command in ['cp']:
            cguid = client_guid()
            do_lineage(
                w,
                cguid=cguid,
                source_path=source_path,
                destination_path=destination
                )
    except Exception as e:
        logging.error(f"Failed to do command or record lineage: {e}")
        exit(5)
