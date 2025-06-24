"""Library shared between kubernetes control plane and kubernetes worker charms."""

import ipaddress
import ops

from typing import Union, List, Literal, overload


_Address = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
_Networks = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
_AddressList = List[_Address]


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
