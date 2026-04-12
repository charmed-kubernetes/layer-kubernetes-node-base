"""Library shared between kubernetes control plane and kubernetes worker charms."""

import logging
import ipaddress
import json
import ops

from typing import Union, List, Literal, overload
import subprocess

log = logging.getLogger(__name__)


_Address = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
_Networks = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
_AddressList = List[_Address]


def addr6_by_interface(if_name: str) -> List[ops.NetworkInterface]:
    """Return all IPv6 addresses on the given network interface using 'ip -6 -j'.

    Args:
        interface (str): The name of the network interface.

    Returns:
        list: List of IPv6 addresses on the interface.
    """
    result: List[ops.NetworkInterface] = []
    try:
        output = subprocess.check_output(
            ["ip", "-6", "-j", "addr", "show", "dev", if_name], encoding="utf-8"
        )
    except subprocess.CalledProcessError as e:
        log.warning(
            "Failed to get IPv6 addresses for interface %s.", if_name, exc_info=e
        )
        return result

    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        log.warning("Failed to decode IPv6 json for interface %s.", if_name, exc_info=e)
        return result

    addr_infos = [info for iface in data for info in iface.get("addr_info", [])]

    for addr_info in addr_infos:
        family: str = addr_info.get("family")
        addr: str = addr_info.get("local")
        prefixlen: int = addr_info.get("prefixlen")
        if (
            family == "inet6" and prefixlen and addr and not addr.startswith("fe80")
        ):  # skip link-local
            info = {
                "address": addr,
                "value": addr,
                "cidr": str(ipaddress.ip_network(f"{addr}/{prefixlen}", strict=False)),
            }
            result.append(ops.NetworkInterface(if_name, info))
    return result


def _by_ver(addresses: _AddressList, version: int) -> _AddressList:
    """Filter addresses by IP version.

    Args:
        addresses (AddressList): set of addresses to filter
        version (int): The IP version to filter by (4 or 6).
    """
    return [ip for ip in addresses if ip.version == version]


def _to_str(addresses: _AddressList) -> List[str]:
    """Convert a list of IP addresses to their string representation.

    Args:
        addresses (AddressList): set of addresses to convert
    """
    return [ip.exploded for ip in addresses]


def _by_subnet_index(ip: _Address, subnets: List[_Networks]) -> int:
    """Return the index of the subnet that most narrowly contains the IP address.

    Args:
        ip (Address): The IP address to check.
        subnets (List[ipaddress._BaseNetwork]): The list of subnets to check against.

    Returns:
        int: The index of the subnet that contains the IP address, or len(subnets) if not found.
    """
    matches = [net for net in subnets if net.version == ip.version and ip in net]
    if matches:
        # Finds the most specific subnet (longest prefix match)
        return subnets.index(max(matches, key=lambda net: net.prefixlen))
    return len(subnets)


@overload
def by_relation(
    charm: ops.CharmBase, relation: str, to_str: Literal[False] = False
) -> _AddressList: ...


@overload
def by_relation(
    charm: ops.CharmBase, relation: str, to_str: Literal[True]
) -> List[str]: ...


def by_relation(
    charm: ops.CharmBase, relation: str, to_str: bool = False
) -> Union[_AddressList, List[str]]:
    """Get a sorted list of unique cluster ip addresses.

    This method retrieves the ingress addresses from the binding if available.
    If no binding is found, it falls back to the relation data.
    It filters out duplicate addresses and sorts them ordering
    IPv4 addresses then IPv6 addresses.

    Args:
        charm (ops.CharmBase): The charm instance.
        relation (str): The relation name to get addresses from.
    """
    addresses, egress_subnets = [], []
    if binding := charm.model.get_binding(relation):
        addresses = binding.network.ingress_addresses
        egress_subnets = binding.network.egress_subnets
        if ifc := next(iter(binding.network.interfaces), None):
            nets = addr6_by_interface(ifc.name)
            addresses.extend(net.address for net in nets if net.address)
            egress_subnets.extend(net.subnet for net in nets if net.subnet)

    if not addresses and (rel := charm.model.get_relation(relation)):
        unit_data = rel.data[charm.model.unit]
        egress_subnet = unit_data.get("egress-subnets")
        address = unit_data.get("ingress-address") or unit_data.get("private-address")
        addresses = [address] if address else []
        egress_subnets = [egress_subnet] if egress_subnet else []

    egress_subnets = [ipaddress.ip_network(addr) for addr in egress_subnets]
    uniq = {ipaddress.ip_address(addr) for addr in addresses}

    # the sort key is
    #   IP version  (4 or 6),
    #   index of matching subnet in egress_subnets
    #   the IP address numerically ordered
    sort = sorted(
        uniq, key=lambda x: (x.version, _by_subnet_index(x, egress_subnets), x)
    )
    return _to_str(sort) if to_str else sort


@overload
def by_relation_preferred(
    charm: ops.CharmBase, relation: str, to_str: Literal[False] = False
) -> _AddressList: ...


@overload
def by_relation_preferred(
    charm: ops.CharmBase, relation: str, to_str: Literal[True]
) -> List[str]: ...


def by_relation_preferred(
    charm: ops.CharmBase, relation: str, to_str: bool = False
) -> Union[_AddressList, List[str]]:
    """Get a list of at most 2 addresses, one per IP version.

    This method retrieves the ingress addresses from the specified relation,
    filters them by IP version (IPv4 and IPv6), and returns a list of the
    first address of each version.

    Args:
        charm (ops.CharmBase): The charm instance.
        relation (str): The relation name to get addresses from.
    """
    addrs: _AddressList = []
    all_addrs = by_relation(charm, relation)
    for ver in (4, 6):
        if ver_addrs := _by_ver(all_addrs, ver):
            addrs.append(ver_addrs[0])
    return _to_str(addrs) if to_str else addrs
