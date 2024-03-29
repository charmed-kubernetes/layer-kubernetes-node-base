"""Library shared between kubernetes control plane and kubernetes worker charms."""

import json
import logging
import time
from os import PathLike
from pathlib import Path
from subprocess import run
from typing import Union, List, Mapping, Optional, Protocol, Tuple

from ops import Model, Object, CharmMeta, StoredState

log = logging.getLogger(__name__)
RUN_RETRIES = 180


class Charm(Protocol):
    def get_node_name(self) -> str: ...  # pragma: no cover

    def get_cloud_name(self) -> str: ...  # pragma: no cover

    model: Model

    meta: CharmMeta


def _is_kubectl(p: PathLike) -> bool:
    """Returns True when the provided path exists."""
    return Path(p).exists()


class LabelMaker(Object):
    """Use to apply labels to a kubernetes node."""

    _stored = StoredState()

    class NodeLabelError(Exception):
        """Raised when there's an error labeling a node."""

        pass

    def __init__(
        self,
        charm: Charm,
        kubeconfig_path: Union[PathLike, str],
        kubectl: Optional[PathLike] = "/snap/bin/kubectl",
    ):
        super().__init__(parent=charm, key="NodeBase")
        self.charm = charm
        self.kubeconfig_path = kubeconfig_path
        self.kubectl_path = kubectl
        self._stored.set_default(current_labels=dict())

    @staticmethod
    def _retried_call(
        cmd: List[str], retry_msg: str, timeout: int = None
    ) -> Tuple[bytes, bytes]:
        timeout = RUN_RETRIES if timeout is None else timeout
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = run(cmd, capture_output=True)
            if rc.returncode == 0:
                return rc.stdout, rc.stderr
            log.error(f"{retry_msg}: {rc.stderr}")
            time.sleep(1)
        else:
            raise LabelMaker.NodeLabelError(retry_msg)

    def _kubectl(self, command: str) -> str:
        if not _is_kubectl(self.kubectl_path):
            retry_msg = "Failed to find kubectl. Will retry."
            stdout, _ = self._retried_call(["which", "kubectl"], retry_msg)
            self.kubectl_path = stdout.decode().strip()

        base = "{0} --kubeconfig={1}".format(self.kubectl_path, self.kubeconfig_path)
        return base + " " + command

    def active_labels(self) -> Optional[Mapping[str, str]]:
        """
        Returns all existing labels if the api server can fetch from the node,
        otherwise returns None indicating the node cannot be relabeled.
        """
        cmd = self._kubectl("get node {0} -o=jsonpath={{.metadata.labels}}")
        cmd = cmd.format(self.charm.get_node_name())
        retry_msg = "Failed to get labels. Will retry."
        try:
            label_json, _ = LabelMaker._retried_call(cmd.split(), retry_msg)
        except LabelMaker.NodeLabelError:
            return None
        try:
            return json.loads(label_json)
        except json.JSONDecodeError:
            log.error(f"Failed to decode labels: {label_json.decode()}")
            return None

    def set_label(self, label: str, value: str) -> None:
        """
        Add a label to this node.

        @param str label: Label name to apply
        @param str value: Value to associate with the label
        @raises LabelMaker.NodeLabelError: if the label cannot be added
        """
        cmd = self._kubectl("label node {0} {1}={2} --overwrite")
        cmd = cmd.format(self.charm.get_node_name(), label, value)
        retry_msg = "Failed to apply label {0}={1}. Will retry.".format(label, value)
        LabelMaker._retried_call(cmd.split(), retry_msg)

    def remove_label(self, label: str) -> None:
        """
        Remove a label to this node.

        @param str label: Label name to remove
        @raises LabelMaker.NodeLabelError: if the label cannot be removed
        """
        cmd = self._kubectl("label node {0} {1}-")
        cmd = cmd.format(self.charm.get_node_name(), label)
        retry_msg = "Failed to remove label {0}. Will retry.".format(label)
        LabelMaker._retried_call(cmd.split(), retry_msg)

    def apply_node_labels(self) -> None:
        """
        Parse the `labels` configuration option and apply the labels to the
        node.

        @raises LabelMaker.NodeLabelError: if the label cannot be added or removed
        """
        # Get the user's configured labels.
        config = self.charm.model.config
        user_labels = {}
        for item in config.get("labels").split(" "):
            try:
                key, val = item.split("=")
            except ValueError:
                log.info(f"Skipping malformed option: {item}.")
            else:
                user_labels[key] = val
        # Collect the current label state.
        current_labels = self._stored.current_labels

        try:
            # Remove any labels that the user has removed from the config.
            for key in list(current_labels.keys()):
                if key not in user_labels:
                    self.remove_label(key)
                    del self._stored.current_labels[key]

            # Add any new labels.
            for key, val in user_labels.items():
                self.set_label(key, val)
                self._stored.current_labels[key] = val

            # Set the juju-application and juju-charm labels.
            self.set_label("juju-application", self.charm.model.app.name)
            self.set_label("juju-charm", self.charm.meta.name)

            # Set the juju.io/cloud label.
            juju_io_cloud_labels = [
                ("aws", "ec2"),
                ("gcp", "gce"),
                ("openstack", "openstack"),
                ("vsphere", "vsphere"),
                ("azure", "azure"),
            ]
            for endpoint, label in juju_io_cloud_labels:
                if endpoint == self.charm.get_cloud_name():
                    self.set_label("juju.io/cloud", label)
                    break
            else:
                # none of the endpoints matched, remove the label
                self.remove_label("juju.io/cloud")

        except self.NodeLabelError as ex:
            log.exception(str(ex))
            raise
