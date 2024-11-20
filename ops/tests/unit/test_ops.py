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
    with mock.patch.object(node_base, "DEFAULT_TIMEOUT", 2):
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

    config_yaml = "options:\n  my-labels:\n    type: string\n    default: ''\n"


@pytest.fixture
def harness():
    harness = ops.testing.Harness(TestCharm, config=TestCharm.config_yaml)
    try:
        harness.begin()
        yield harness
    finally:
        harness.cleanup()


@pytest.fixture(autouse=True)
def is_kubectl():
    with mock.patch.object(node_base, "_is_kubectl", return_value=True) as the_mock:
        yield the_mock


@pytest.fixture
def label_maker(harness) -> node_base.LabelMaker:
    return node_base.LabelMaker(harness.charm, KUBE_CONFIG, user_label_key="my-labels")


def test_active_labels_no_api(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse()
    assert label_maker.active_labels() is None


def test_active_labels_invalid_kubectl(subprocess_run, label_maker, is_kubectl):
    is_kubectl.return_value = False
    subprocess_run.return_value = RunResponse(1, b"", b"")
    with pytest.raises(node_base.LabelMaker.NodeLabelError):
        assert label_maker.active_labels() is None


def test_active_labels_invalid_kubectl_response(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse(0, b"--")
    assert label_maker.active_labels() is None


def test_active_labels_no_labels(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse(0, b"{}")
    assert label_maker.active_labels() == {}


def test_active_labels_single_label(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse(
        0, b'{"node-role.kubernetes.io/control-plane": ""}'
    )
    assert label_maker.active_labels() == {"node-role.kubernetes.io/control-plane": ""}


def test_active_labels_apply_layer_failure(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse(1)
    with pytest.raises(node_base.LabelMaker.NodeLabelError):
        label_maker.apply_node_labels()


def test_active_labels_apply_layers_with_cloud(subprocess_run, label_maker):
    subprocess_run.return_value = RunResponse(0)
    with mock.patch.object(TestCharm, "CLOUD", "aws"):
        label_maker.apply_node_labels()
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


def test_active_labels_apply_layers_from_config(
    subprocess_run, harness, label_maker, caplog
):
    harness.update_config(
        {"my-labels": "node-role.kubernetes.io/control-plane= invalid"}
    )
    subprocess_run.return_value = RunResponse(0)
    label_maker._stored.current_labels = {"node-role.kubernetes.io/worker": ""}
    label_maker.apply_node_labels()
    assert "Skipping malformed option: invalid." in caplog.messages
    assert label_maker._stored.current_labels == {
        "node-role.kubernetes.io/control-plane": ""
    }
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
