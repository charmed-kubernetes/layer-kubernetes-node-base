import ipaddress
from dataclasses import dataclass
from typing import Optional
import pytest
import unittest.mock as mock

from charms import node_base
import charms.node_base.address as node_address
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
    with mock.patch.object(node_base.labels, "DEFAULT_TIMEOUT", 2):
        yield


@pytest.fixture
def subprocess_run(fast_retry):
    with mock.patch("charms.node_base.labels.run") as mock_run:
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
    with mock.patch.object(
        node_base.labels, "_is_kubectl", return_value=True
    ) as the_mock:
        yield the_mock


@pytest.fixture
def label_maker(harness) -> node_base.LabelMaker:
    return node_base.LabelMaker(harness.charm, KUBE_CONFIG, user_label_key="my-labels")


@pytest.mark.parametrize(
    "label, expected",
    [
        ("key-a=val-a", {"key-a": "val-a"}),
        ("key-a=val-a key-b=val-b", {"key-a": "val-a", "key-b": "val-b"}),
        ("  key-a=val-a key-b=val-b", {"key-a": "val-a", "key-b": "val-b"}),
    ],
)
def test_user_labels(harness, label_maker, caplog, label, expected):
    harness.update_config({"my-labels": label})
    assert not label_maker._raise_invalid_label
    assert label_maker.user_labels() == expected
    assert caplog.messages == []


def test_user_labels_invalid(harness, label_maker, caplog):
    label = " key-a key-b"
    harness.update_config({"my-labels": label})

    label_maker._raise_invalid_label = True
    with pytest.raises(node_base.LabelMaker.NodeLabelError):
        label_maker.user_labels()
    assert caplog.messages == []

    label_maker._raise_invalid_label = False
    assert label_maker.user_labels() == {}
    assert caplog.messages == [
        "Skipping Malformed label: key-a.",
        "Skipping Malformed label: key-b.",
    ]


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
    juju_az = "test-az-1"

    def getenv_se(key: str) -> Optional[str]:
        """os.getenv side effect."""
        return juju_az if key == "JUJU_AVAILABILITY_ZONE" else ""

    subprocess_run.return_value = RunResponse(0)
    # NOTE(Hue): using nested mocks since parenthesized context managers is not
    # supported in Python 3.8
    with mock.patch.object(TestCharm, "CLOUD", "aws"):
        with mock.patch("charms.node_base.labels.os.getenv") as mock_getenv:
            mock_getenv.side_effect = getenv_se
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
                (f"topology.kubernetes.io/zone={juju_az}", "--overwrite"),
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
        {
            "my-labels": "node-role.kubernetes.io/control-plane= invalid extra-label.removable-"
        }
    )
    subprocess_run.return_value = RunResponse(0)
    label_maker._stored.current_labels = {
        "node-role.kubernetes.io/worker": "",
        "extra-label.removable": "",
    }
    label_maker.apply_node_labels()
    assert "Skipping Malformed label: invalid." in caplog.messages
    assert label_maker._stored.current_labels == {
        "node-role.kubernetes.io/control-plane": "",
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
                ("extra-label.removable-",),
                ("node-role.kubernetes.io/control-plane=", "--overwrite"),
                ("juju-application=test-charm", "--overwrite"),
                ("juju-charm=test-charm", "--overwrite"),
                ("juju.io/cloud-",),
            ]
        ],
    )


def test_raise_invalid_label(subprocess_run, harness, label_maker):
    harness.update_config({"my-labels": "this=isn't=valid"})
    subprocess_run.return_value = RunResponse(0)
    label_maker._raise_invalid_label = True
    with pytest.raises(node_base.LabelMaker.NodeLabelError):
        label_maker.apply_node_labels()


@pytest.mark.parametrize("to_str", [False, True], ids=["objects", "strings"])
@pytest.mark.parametrize(
    "bind_addresses, unit_data, expected_all, expected_preferred",
    [
        (
            [],
            {"ingress-address": "10.2.3.4", "egress-subnets": "10.0.0.0/8"},
            ["10.2.3.4"],
            ["10.2.3.4"],
        ),  # by unit data ingress-address and egress-subnets
        (
            [],
            {"private-address": "10.2.3.4", "egress-subnets": "10.0.0.0/8"},
            ["10.2.3.4"],
            ["10.2.3.4"],
        ),  # by unit data private-address and egress-subnets
        (
            ["10.2.3.4"],
            {},
            ["10.2.3.4"],
            ["10.2.3.4"],
        ),  # single bind address matching the egress-subnets
        (
            [
                ipaddress.ip_address("250.0.0.1"),
                "10.2.3.4",
            ],
            {},
            ["10.2.3.4", "250.0.0.1"],  # sorted order
            ["10.2.3.4"],
        ),  # multiple bind addresses, one matching the egress-subnets
        (
            ["250.0.0.1", "10.2.3.4", "1.0.0.1"],
            {},
            ["10.2.3.4", "1.0.0.1", "250.0.0.1"],
            ["10.2.3.4"],
        ),  # multiple bind addresses, one matching the egress-subnets
        (
            [ipaddress.ip_address("ffc0::1"), "10.2.3.4"],
            {},
            ["10.2.3.4", "ffc0:0000:0000:0000:0000:0000:0000:0001"],
            ["10.2.3.4", "ffc0:0000:0000:0000:0000:0000:0000:0001"],
        ),
        (
            [
                ipaddress.ip_address("ffc0::2"),
                ipaddress.ip_address("ffc0::1"),
                "10.2.3.4",
                "250.0.0.1",
            ],
            {},
            [
                "10.2.3.4",
                "250.0.0.1",
                "ffc0:0000:0000:0000:0000:0000:0000:0001",
                "ffc0:0000:0000:0000:0000:0000:0000:0002",
            ],
            ["10.2.3.4", "ffc0:0000:0000:0000:0000:0000:0000:0001"],
        ),
    ],
    ids=[
        "by-unit-data-ingress",
        "by-unit-data-private",
        "single-bind-address",
        "multiple-bind-addresses",
        "multiple-bind-sorted-by-egress-subnet-then-numerically",
        "ipv6-ipv4-mixed",
        "ipv6-mulit-ipv4-mixed",
    ],
)
@mock.patch(
    "charms.node_base.address.addr6_by_interface", new=mock.MagicMock(return_value=[])
)
def test_node_address_by_relation(
    bind_addresses, unit_data, expected_all, expected_preferred, to_str
):
    charm = mock.MagicMock()
    charm.model.unit = "my-unit/0"
    binding = charm.model.get_binding.return_value
    binding.network.interfaces = [mock.MagicMock(name="eth0")]
    binding.network.ingress_addresses = bind_addresses
    binding.network.egress_subnets = ["10.0.0.0/8"]
    relation = charm.model.get_relation.return_value
    relation.data = {charm.model.unit: unit_data}

    relation_name = "my-relation"
    actual = node_address.by_relation(charm, relation_name, to_str)
    charm.model.get_binding.assert_called_once_with(relation_name)
    if not to_str:
        expected_all = [ipaddress.ip_address(fmt) for fmt in expected_all]
    assert actual == expected_all

    charm.model.get_binding.reset_mock()
    actual = node_address.by_relation_preferred(charm, relation_name, to_str)
    charm.model.get_binding.assert_called_once_with(relation_name)
    if not to_str:
        expected_preferred = [ipaddress.ip_address(fmt) for fmt in expected_preferred]
    assert actual == expected_preferred


ADDRV6_OUTPUT = """
[{"ifindex":2,"ifname":"enp5s0","flags":["BROADCAST","MULTICAST","UP","LOWER_UP"],"mtu":1500,"qdisc":"mq","operstate":"UP","group":"default","txqlen":1000,"addr_info":[{"family":"inet6","local":"fd42:270:c358:cd3b:216:3eff:fe69:5bca","prefixlen":64,"scope":"global","mngtmpaddr":true,"noprefixroute":true,"valid_life_time":4294967295,"preferred_life_time":4294967295},{"family":"inet6","local":"fe80::216:3eff:fe69:5bca","prefixlen":64,"scope":"link","valid_life_time":4294967295,"preferred_life_time":4294967295}]}]
""".strip()


@mock.patch("subprocess.check_output", new=mock.MagicMock(return_value=ADDRV6_OUTPUT))
def test_addr6_by_interface():
    expected = [
        ops.NetworkInterface(
            "enp5s0",
            {
                "address": "fd42:270:c358:cd3b:216:3eff:fe69:5bca",
                "value": "fd42:270:c358:cd3b:216:3eff:fe69:5bca",
                "cidr": "fd42:270:c358:cd3b::/64",
            },
        ),
    ]
    actual = node_address.addr6_by_interface("enp5s0")
    for each, expected_each in zip(actual, expected):
        assert each.name == expected_each.name
        assert each.address == expected_each.address
        assert each.subnet == expected_each.subnet
