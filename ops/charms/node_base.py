"""Library shared between kubernetes control plane and kubernetes worker charms."""

import json
import logging
from subprocess import run
from os import PathLike
import time
from typing import Union, List, Mapping, Optional, Protocol, Tuple

from ops import Model, Object, CharmMeta, StoredState

log = logging.getLogger(__name__)


class Charm(Protocol):
    def get_node_name(self) -> str:
        ...

    def get_cloud_name(self) -> str:
        ...

    model: Model

    meta: CharmMeta


class LabelMaker(Object):
    """Use to apply labels to a kubernetes node."""

    _stored = StoredState()

    class NodeLabelError(Exception):
        """Raised when there's an error labeling a node."""

        pass

    def __init__(self, charm: Charm, kubeconfig_path: Union[PathLike, str]):
        super().__init__(parent=charm, key="NodeBase")
        self.kubeconfig_path = kubeconfig_path
        self.charm = charm
        self._stored.set_default(current_labels=dict())

    @staticmethod
    def _retried_call(
        cmd: List[str], retry_msg: str, timeout: int = 180
    ) -> Tuple[str, str]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = run(cmd)
            if rc.returncode == 0:
                return rc.stdout, rc.stderr
            log.info(retry_msg)
            time.sleep(1)
        else:
            raise LabelMaker.NodeLabelError(retry_msg)

    def active_labels(self) -> Optional[Mapping[str, str]]:
        """
        Returns all existing labels if the api server can fetch from the node,
        otherwise returns None indicating the node cannot be relabeled.
        """
        cmd = "kubectl --kubeconfig={0} get node {1} -o=jsonpath='{{.metadata.labels}}"
        cmd = cmd.format(self.kubeconfig_path, self.charm.get_node_name())
        retry_msg = "Failed to get labels. Will retry."
        try:
            label_json = LabelMaker._retried_call(cmd.split(), retry_msg)
        except LabelMaker.NodeLabelError:
            return None
        try:
            return json.loads(label_json)
        except json.JSONDecodeError:
            return None

    def set_label(self, label: str, value: str) -> None:
        """
        Add a label to this node.

        @param str label: Label name to apply
        @param str value: Value to associate with the label
        @raises LabelMaker.NodeLabelError: if the label cannot be added
        """
        cmd = "kubectl --kubeconfig={0} label node {1} {2}={3} --overwrite"
        cmd = cmd.format(self.kubeconfig_path, self.charm.get_node_name(), label, value)
        retry_msg = "Failed to apply label {0}={1}. Will retry.".format(label, value)
        LabelMaker._retried_call(cmd.split(), retry_msg)

    def remove_label(self, label: str) -> None:
        """
        Remove a label to this node.

        @param str label: Label name to remove
        @raises LabelMaker.NodeLabelError: if the label cannot be removed
        """
        cmd = "kubectl --kubeconfig={0} label node {1} {2}-"
        cmd = cmd.format(self.kubeconfig_path, self.charm.get_node_name(), label)
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
