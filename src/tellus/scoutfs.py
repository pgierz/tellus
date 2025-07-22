import datetime
import time

import fsspec.implementations.sftp
import requests
from fsspec.registry import register_implementation
from loguru import logger
from rich.console import Console
from rich.live import Live
from rich.text import Text


class ScoutFSFileSystem(fsspec.implementations.sftp.SFTPFileSystem):
    protocol = "scoutfs", "sftp", "ssh"

    def __init__(self, host, **kwargs):
        self._scoutfs_config = kwargs.pop("scoutfs_config", {})
        ssh_kwargs = kwargs
        super().__init__(host, **ssh_kwargs)

    def _scoutfs_generate_token(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        # [FIXME] Use environment variables or some other secure method to
        #         store credentials!
        data = {
            "acct": "filestat",
            "pass": "filestat",
        }

        # Ignore SSL certificate warnings (verify=False)
        response = requests.post(
            f"{self._scoutfs_api_url}/security/login",
            headers=headers,
            json=data,
            verify=False,
        )
        response.raise_for_status()
        return response.json().get("response")

    @property
    def _scoutfs_token(self):
        if "token" not in self._scoutfs_config:
            self._scoutfs_config["token"] = self._scoutfs_generate_token()
        return self._scoutfs_config["token"]

    @property
    def _scoutfs_api_url(self):
        return self._scoutfs_config.get("api_url", "https://hsm.dmawi.de:8080/v1")

    def _scoutfs_get_filesystems(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        response = requests.get(
            f"{self._scoutfs_api_url}/filesystems",
            headers=headers,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def _get_fsid_for_path(self, path):
        fsid_response = self._scoutfs_get_filesystems()
        matching_fsids = []
        for fsid_info in fsid_response.get("fsids", []):
            if path.startswith(fsid_info["mount"]):
                matching_fsids.append(fsid_info)
        assert len(matching_fsids) == 1, (
            f"Expected exactly one matching filesystem for path '{path}', "
            f"found {len(matching_fsids)}: {matching_fsids}"
        )
        fsid = matching_fsids[0]["fsid"]
        return fsid

    def _scoutfs_file(self, path):
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        response = requests.get(
            f"{self._scoutfs_api_url}/file?fsid={fsid}&path={path}",
            headers=headers,
            # json=params,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def _scoutfs_batchfile(self, paths):
        # [FIXME] For Malte: thisd doesn't work yet, HSM docu says:
        # "This documentation has not been updated for the use of multiple fs (fsid) and "batch"-commands yet..."
        #
        # ...whatever that means.
        if isinstance(paths, str):
            paths = [paths]
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        params = {"paths": paths}
        response = requests.put(
            f"{self._scoutfs_api_url}/batchfile",
            headers=headers,
            json=params,
            verify=False,
        )
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            return None

    def _scoutfs_request(self, command, path):
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        params = {"path": path}
        response = requests.post(
            f"{self._scoutfs_api_url}/request/{command}?fsid={fsid}&path={path}",
            headers=headers,
            json=params,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def _scoutfs_queues(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        response = requests.get(
            f"{self._scoutfs_api_url}/queues",
            headers=headers,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    @property
    def queues(self):
        return self._scoutfs_queues()

    # [TODO] Not sure if this is a "private" method or not.
    def stage(self, path):
        return self._scoutfs_request("stage", path)

    def info(self, path):
        robj = super().info(path)
        # Add ScoutFS-specific information
        scoutfs_file = self._scoutfs_file(path)
        robj["scoutfs_info"] = {
            "/file": scoutfs_file,
            "/batchfile": None,
        }
        return robj

    def is_online(self, path):
        info = self.info(path)
        online_blocks = info["scoutfs_info"]["/file"].get("onlineblocks", "")
        offline_blocks = info["scoutfs_info"]["/file"].get("offlineblocks", "")
        if online_blocks != "":
            online_blocks = int(online_blocks)
        if offline_blocks != "":
            offline_blocks = int(offline_blocks)
        # [FIXME]: Partially online files might be mis-represented here?
        return online_blocks > 0 and offline_blocks == 0

    def _scoutfs_online_status(self, path):
        info = self.info(path)
        online_blocks = info["scoutfs_info"]["/file"].get("onlineblocks", "")
        offline_blocks = info["scoutfs_info"]["/file"].get("offlineblocks", "")
        if online_blocks != "":
            online_blocks = int(online_blocks)
        if offline_blocks != "":
            offline_blocks = int(offline_blocks)
        rval = Text.from_markup(
            f"{path} [green]online_blocks: {online_blocks}[/green], [red]offline_blocks: {offline_blocks}[/red]"
        )
        # logger.debug(rval)
        return rval

    def open(
        self,
        path,
        mode="r",
        stage_before_opening=True,
        timeout=None,
        **kwargs,
    ):
        if stage_before_opening:
            if not self.is_online(path):
                self.stage(path)
            # [FIXME] Is this blocking? Should it be...?
            timeout = timeout or datetime.datetime.now() + datetime.timedelta(minutes=3)
            console = Console()
            with console.status("[bold green] Staging file...", spinner="dots"):
                # [TODO] Progress bar would be nice
                console.print(f"{datetime.datetime.now()} Current status: ")
                console.print(self._scoutfs_online_status(path))
                while not self.is_online(path):
                    time.sleep(1)
                    if self.is_online(path):
                        break
                    if datetime.datetime.now() > timeout:
                        raise TimeoutError(
                            f"Timeout while waiting for file {path} to be staged."
                        )
        else:
            # Let the filesystem handle staging somehow outside of our world...
            pass
        return super().open(path, mode, **kwargs)


register_implementation("scoutfs", ScoutFSFileSystem)
