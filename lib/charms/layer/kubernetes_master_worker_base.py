"""Library shared between kubernetes control plane and kubernetes worker charms."""

from subprocess import call
from os import PathLike
import time
from typing import Union, List

from charms.layer.kubernetes_common import get_node_name
from charms.reactive import is_state
from charmhelpers.core import hookenv, unitdata

db = unitdata.kv()


class LabelMaker:
    """Use to apply labels to a kubernetes node."""

    class NodeLabelError(Exception):
        """Raised when there's an error labeling a node."""

        pass

    def __init__(self, kubeconfig_path: Union[PathLike, str]):
        self.kubeconfig_path = kubeconfig_path
        self.node = get_node_name()

    @staticmethod
    def _retried_call(cmd: List[str], retry_msg: str, timeout: int = 180) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            code = call(cmd)
            if code == 0:
                return True
            hookenv.log(retry_msg)
            time.sleep(1)
        else:
            return False

    def set_label(self, label: str, value: str) -> None:
        """
        Add a label to this node.

        @param str label: Label name to apply
        @param str value: Value to associate with the label
        @raises LabelMaker.NodeLabelError: if the label cannot be added
        """
        cmd = "kubectl --kubeconfig={0} label node {1} {2}={3} --overwrite"
        cmd = cmd.format(self.kubeconfig_path, self.node, label, value)
        retry_msg = "Failed to apply label {0}={1}. Will retry.".format(label, value)
        if not LabelMaker._retried_call(cmd.split(), retry_msg):
            raise LabelMaker.NodeLabelError(retry_msg)

    def remove_label(self, label: str) -> None:
        """
        Remove a label to this node.

        @param str label: Label name to remove
        @raises LabelMaker.NodeLabelError: if the label cannot be removed
        """
        cmd = "kubectl --kubeconfig={0} label node {1} {2}-"
        cmd = cmd.format(self.kubeconfig_path, self.node, label)
        retry_msg = "Failed to remove label {0}. Will retry.".format(label)
        if not LabelMaker._retried_call(cmd.split(), retry_msg):
            raise LabelMaker.NodeLabelError(retry_msg)

    def apply_node_labels(self) -> None:
        """
        Parse the `labels` configuration option and apply the labels to the
        node.

        @raises LabelMaker.NodeLabelError: if the label cannot be added or removed
        """
        # Get the user's configured labels.
        config = hookenv.config()
        user_labels = {}
        for item in config.get("labels").split(" "):
            try:
                key, val = item.split("=")
            except ValueError:
                hookenv.log("Skipping malformed option: {}.".format(item))
            else:
                user_labels[key] = val
        # Collect the current label state.
        current_labels = db.get("current_labels") or {}

        try:
            # Remove any labels that the user has removed from the config.
            for key in list(current_labels.keys()):
                if key not in user_labels:
                    self.remove_label(key)
                    del current_labels[key]
                    db.set("current_labels", current_labels)

            # Add any new labels.
            for key, val in user_labels.items():
                self.set_label(key, val)
                current_labels[key] = val
                db.set("current_labels", current_labels)

            # Set the juju-application label.
            self.set_label("juju-application", hookenv.service_name())

            # Set the juju.io/cloud label.
            juju_io_cloud_labels = [
                ("aws", "ec2"),
                ("gcp", "gce"),
                ("openstack", "openstack"),
                ("vsphere", "vsphere"),
                ("azure", "azure"),
            ]
            for endpoint, label in juju_io_cloud_labels:
                if is_state("endpoint.{0}.ready".format(endpoint)):
                    self.set_label("juju.io/cloud", label)
                    break
            else:
                # none of the endpoints matched, remove the label
                self.remove_label("juju.io/cloud")

        except self.NodeLabelError as ex:
            hookenv.log(str(ex))
            raise
