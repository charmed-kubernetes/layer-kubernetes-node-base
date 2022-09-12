import unittest.mock as mock
from reactive import kubernetes_node_base


@mock.patch("os.listdir")
@mock.patch("os.replace")
@mock.patch("os.path.exists", mock.Mock(return_value=True))
def test_upgrade_charm_renames_config(mock_os_replace, mock_os_list_dir):
    mock_os_list_dir.return_value = [
        "99-something.conf",
        "05-default.conf",
    ]
    kubernetes_node_base.upgrade_charm()
    mock_os_replace.assert_called_once_with(
        "/etc/cni/net.d/05-default.conf",
        "/etc/cni/net.d/01-default.conf",
    )
