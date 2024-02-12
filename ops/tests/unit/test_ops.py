from dataclasses import dataclass
import pytest
import unittest.mock as mock

from charms import node_base
import ops
import ops.testing

KUBE_CONFIG = "/home/ubuntu/.kube/config"


@dataclass
class RunResponse:
    returncode: int = -1
    stdout: bytes = b""
    stderr: bytes = b""


@pytest.fixture
def fast_retry():
    with mock.patch.object(node_base, "RUN_RETRIES", 2):
        yield


@pytest.fixture
def subprocess_run(fast_retry):
    with mock.patch("charms.node_base.run") as mock_run:
        yield mock_run


class TestCharm(ops.CharmBase):
    HOSTNAME = "my-hostname"
    CLOUD = "my-cloud"

    def get_node_name(self):
        return self.HOSTNAME

    def get_cloud_name(self):
        return self.CLOUD

    config_yaml = "options:\n  labels:\n    type: string\n    default: ''\n"


@pytest.fixture
def harness():
    harness = ops.testing.Harness(TestCharm, config=TestCharm.config_yaml)
    try:
        harness.begin()
        yield harness
    finally:
        harness.cleanup()


def test_active_labels_no_api(subprocess_run, harness):
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    subprocess_run.return_value = RunResponse()
    assert lm.active_labels() is None


def test_active_labels_invalid_kubectl(subprocess_run, harness):
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    subprocess_run.return_value = RunResponse(0, b"--")
    assert lm.active_labels() is None


def test_active_labels_no_labels(subprocess_run, harness):
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    subprocess_run.return_value = RunResponse(0, b"{}")
    assert lm.active_labels() == {}


def test_active_labels_single_label(subprocess_run, harness):
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    subprocess_run.return_value = RunResponse(
        0, b'{"node-role.kubernetes.io/control-plane": ""}'
    )
    assert lm.active_labels() == {"node-role.kubernetes.io/control-plane": ""}


def test_active_labels_apply_layer_failure(subprocess_run, harness):
    subprocess_run.return_value = RunResponse(1)
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    with pytest.raises(node_base.LabelMaker.NodeLabelError):
        lm.apply_node_labels()


def test_active_labels_apply_layers_with_cloud(subprocess_run, harness):
    subprocess_run.return_value = RunResponse(0)
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    with mock.patch.object(TestCharm, "CLOUD", "aws"):
        lm.apply_node_labels()
    subprocess_run.assert_has_calls(
        [
            mock.call(
                [
                    "/snap/bin/kubectl",
                    f"--kubeconfig={KUBE_CONFIG}",
                    "label",
                    "node",
                    "my-hostname",
                    *label_args,
                ],
                capture_output=True,
            )
            for label_args in [
                ("juju-application=test-charm", "--overwrite"),
                ("juju-charm=test-charm", "--overwrite"),
                ("juju.io/cloud=ec2", "--overwrite"),
            ]
        ],
    )


def test_active_labels_apply_layers_from_config(subprocess_run, harness, caplog):
    harness.update_config({"labels": "node-role.kubernetes.io/control-plane= invalid"})
    subprocess_run.return_value = RunResponse(0)
    lm = node_base.LabelMaker(harness.charm, KUBE_CONFIG)
    lm._stored.current_labels = {"node-role.kubernetes.io/worker": ""}
    lm.apply_node_labels()
    assert "Skipping malformed option: invalid." in caplog.messages
    assert lm._stored.current_labels == {"node-role.kubernetes.io/control-plane": ""}
    subprocess_run.assert_has_calls(
        [
            mock.call(
                [
                    "/snap/bin/kubectl",
                    f"--kubeconfig={KUBE_CONFIG}",
                    "label",
                    "node",
                    "my-hostname",
                    *label_args,
                ],
                capture_output=True,
            )
            for label_args in [
                ("node-role.kubernetes.io/worker-",),
                ("node-role.kubernetes.io/control-plane=", "--overwrite"),
                ("juju-application=test-charm", "--overwrite"),
                ("juju-charm=test-charm", "--overwrite"),
                ("juju.io/cloud-",),
            ]
        ],
    )
