import pytest
import unittest.mock as mock

from charms.layer import kubernetes_master_worker_base
from charmhelpers.core import hookenv


class TestNodeLabels:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, request):
        self.kube_control = mock.Mock()
        self.config = {"labels": f'{request.node.name}="value"'}

        hc = mock.Mock()
        hc.side_effect = lambda k=None: self.config[k] if k else self.config
        monkeypatch.setattr(hookenv, "config", hc)

        self.hook_log = mock.Mock()
        monkeypatch.setattr(hookenv, "log", self.hook_log)

        hsn = mock.Mock(return_value="kubernetes-control-plane")
        monkeypatch.setattr(hookenv, "service_name", hsn)

        gnn = mock.Mock(return_value="the-node")
        monkeypatch.setattr(kubernetes_master_worker_base, "get_node_name", gnn)

        mock_call = self.call = mock.Mock(return_value=0)
        monkeypatch.setattr(kubernetes_master_worker_base, "call", mock_call)

        self.base_node_cmd = [
            "kubectl",
            "--kubeconfig=/path/to/kube/config",
            "label",
            "node",
            "the-node",
        ]

    def test_label_add(self, request):
        label_maker = kubernetes_master_worker_base.LabelMaker("/path/to/kube/config")
        label_maker.apply_node_labels()

        call_set = [
            mock.call(self.base_node_cmd + expected)
            for expected in [
                [f'{request.node.name}="value"', "--overwrite"],
                ["juju-application=kubernetes-control-plane", "--overwrite"],
                ["juju.io/cloud-"],
            ]
        ]
        self.call.assert_has_calls(call_set, any_order=False)

    def test_invalid_label(self):
        self.config = {"labels": "too=many=equals not_enough_equals"}
        label_maker = kubernetes_master_worker_base.LabelMaker("/path/to/kube/config")
        label_maker.apply_node_labels()
        call_set = [
            mock.call(self.base_node_cmd + expected)
            for expected in [
                ["juju-application=kubernetes-control-plane", "--overwrite"],
                ["juju.io/cloud-"],
            ]
        ]
        self.call.assert_has_calls(call_set, any_order=False)

        call_set = [
            mock.call("Skipping malformed option: too=many=equals."),
            mock.call("Skipping malformed option: not_enough_equals."),
        ]
        self.hook_log.assert_has_calls(call_set, any_order=False)
