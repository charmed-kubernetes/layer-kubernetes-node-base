"""Library shared between kubernetes control plane and kubernetes worker charms."""

import json
import logging
import os
import shlex
import ops
from subprocess import run
from os import PathLike
from pathlib import Path

import time
from typing import Union, List, Mapping, Optional, Protocol, Tuple

try:
    from typing import Annotated, TypeAlias

    PositiveInt: TypeAlias = Annotated[int, lambda x: x > 0]
except ImportError:
    # if Annotated is not available, just use int
    PositiveInt = int

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 180
TOPOLOGY_NODE_LABEL = "topology.kubernetes.io/zone"
JUJU_AVAILABILITY_ZONE = "JUJU_AVAILABILITY_ZONE"


class Charm(Protocol):
    def get_node_name(self) -> str: ...  # pragma: no cover

    def get_cloud_name(self) -> str: ...  # pragma: no cover

    model: ops.Model
    meta: ops.CharmMeta


def _is_kubectl(p: PathLike) -> bool:
    """Returns True when the provided path exists.

    Args:
        p (PathLike): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    return Path(p).exists()


class LabelMaker(ops.Object):
    """Use to apply labels to a kubernetes node."""

    _stored = ops.StoredState()

    class NodeLabelError(Exception):
        """Raised when there's an error labeling a node."""

        pass

    def __init__(
        self,
        charm: Charm,
        kubeconfig_path: Union[PathLike, str],
        *,
        kubectl: Optional[PathLike] = "/snap/bin/kubectl",
        user_label_key: str = "labels",
        timeout: Optional[PositiveInt] = None,
        raise_invalid_label: bool = False,
    ) -> None:
        """Initialize the LabelMaker.

        Args:
            charm (Charm): The charm instance.
            kubeconfig_path (Union[PathLike, str]): Path to the kubeconfig file.
            kubectl (Optional[PathLike], optional): Path to the kubectl binary. Defaults to "/snap/bin/kubectl".
            user_label_key (str, optional): The key in the charm config where the user labels are stored. Defaults to "labels".
            timeout (Optional[PositiveInt], optional): Number of seconds to retry a command. Defaults to None.
            raise_invalid_label (bool, optional): Whether to raise an exception when an invalid label is found. Defaults to False.
        """
        super().__init__(parent=charm, key="NodeBase")
        self.charm = charm
        self.kubeconfig_path = kubeconfig_path
        self.kubectl_path = kubectl
        self.user_labels_key = user_label_key
        self.timeout = DEFAULT_TIMEOUT if timeout is None else timeout
        self._stored.set_default(current_labels=dict())
        self._raise_invalid_label = raise_invalid_label

    def _retried_call(
        self, cmd: List[str], retry_msg: str, timeout: Optional[int] = None
    ) -> Tuple[bytes, bytes]:
        """Run a command with retries.

        Args:
            cmd (List[str]): The command to run.
            retry_msg (str): The message to log on retry.
            timeout (Optional[int], optional): The timeout for retries. Defaults to None.

        Returns:
            Tuple[bytes, bytes]: The stdout and stderr of the command.

        Raises:
            LabelMaker.NodeLabelError: If the command fails after retries.
        """
        if timeout is None:
            timeout = self.timeout
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
        """Construct a kubectl command.

        Args:
            command (str): The kubectl command to run.

        Returns:
            str: The full kubectl command.
        """
        if not _is_kubectl(self.kubectl_path):
            retry_msg = "Failed to find kubectl. Will retry."
            stdout, _ = self._retried_call(["which", "kubectl"], retry_msg)
            self.kubectl_path = stdout.decode().strip()

        base = "{0} --kubeconfig={1}".format(self.kubectl_path, self.kubeconfig_path)
        return base + " " + command

    def active_labels(self) -> Optional[Mapping[str, str]]:
        """Returns all existing labels if the api server can fetch from the node,
        otherwise returns None indicating the node cannot be relabeled.

        Returns:
            Optional[Mapping[str, str]]: The existing labels or None.
        """
        cmd = self._kubectl("get node {0} -o=jsonpath={{.metadata.labels}}")
        cmd = cmd.format(self.charm.get_node_name())
        retry_msg = "Failed to get labels. Will retry."
        try:
            label_json, _ = self._retried_call(shlex.split(cmd), retry_msg)
        except LabelMaker.NodeLabelError:
            return None
        try:
            return json.loads(label_json)
        except json.JSONDecodeError:
            log.error(f"Failed to decode labels: {label_json.decode()}")
            return None

    def set_label(self, label: str, value: str) -> None:
        """Add a label to this node.

        Args:
            label (str): Label name to apply.
            value (str): Value to associate with the label.

        Raises:
            LabelMaker.NodeLabelError: If the label cannot be added.
        """
        cmd = self._kubectl("label node {0} {1}={2} --overwrite")
        cmd = cmd.format(self.charm.get_node_name(), label, value)
        retry_msg = "Failed to apply label {0}={1}. Will retry.".format(label, value)
        self._retried_call(shlex.split(cmd), retry_msg)

    def remove_label(self, label: str) -> None:
        """Remove a label from this node.

        Args:
            label (str): Label name to remove.

        Raises:
            LabelMaker.NodeLabelError: If the label cannot be removed.
        """
        cmd = self._kubectl("label node {0} {1}-")
        cmd = cmd.format(self.charm.get_node_name(), label)
        retry_msg = "Failed to remove label {0}. Will retry.".format(label)
        self._retried_call(shlex.split(cmd), retry_msg)

    def user_labels(self) -> Mapping[str, str]:
        """Returns the labels configured by the user.

        Returns:
            Mapping[str, str]: User configured labels.
        """
        user_labels, data = {}, self.charm.model.config[self.user_labels_key]
        for item in data.split(" "):
            try:
                key, val = item.split("=")
            except ValueError:
                if self._raise_invalid_label:
                    raise self.NodeLabelError(f"Malformed label: {item}.")
                log.error(f"Skipping Malformed label: {item}.")
            else:
                user_labels[key] = val
        return user_labels

    def get_label(self, key: str) -> Optional[str]:
        """Get the value of a label.

        Args:
            key (str): The label to get.

        Returns:
            Optional[str]: The value of the label or None.
        """
        labels = self.active_labels()
        if labels is None:
            return None
        return labels.get(key)

    def apply_node_labels(self) -> None:
        """Parse the `labels` configuration option and apply the labels to the node.

        Raises:
            LabelMaker.NodeLabelError: If the label cannot be added or removed.
        """
        # Get the user's configured labels.
        user_labels = self.user_labels()
        # Collect the current label state.
        current_labels = self._stored.current_labels

        juju_az = os.getenv(JUJU_AVAILABILITY_ZONE)
        if juju_az and not self.get_label(TOPOLOGY_NODE_LABEL):
            self.set_label(TOPOLOGY_NODE_LABEL, juju_az)

        try:
            # Remove any labels that the user has removed from the config.
            for key in list(current_labels.keys()):
                if key not in user_labels:
                    self.remove_label(key)
                    del self._stored.current_labels[key]

            # Add any new labels.
            for key, val in user_labels.items():
                if val.endswith("-"):
                    # Remove the label if the value ends with a dash.
                    self.remove_label(key)
                else:
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
