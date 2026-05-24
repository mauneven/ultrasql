//! PostgreSQL network-address runtime payloads.
//!
//! `INET` and `CIDR` keep the address family, address bytes, and prefix
//! length needed for PostgreSQL containment operators. `MACADDR` and
//! `MACADDR8` keep normalized octets and render with lower-case colon
//! separators.

use std::cmp::Ordering;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::DataType;

const IPV4_BITS: u8 = 32;
const IPV6_BITS: u8 = 128;
const PGSQL_AF_INET: u8 = 2;
const PGSQL_AF_INET6: u8 = 3;

/// SQL `INET` / `CIDR` address plus prefix length.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InetAddr {
    addr: IpAddr,
    prefix: u8,
}

impl InetAddr {
    /// Parse an `INET` literal. Host bits are preserved.
    #[must_use]
    pub fn parse_inet(text: &str) -> Option<Self> {
        let (addr, prefix) = parse_ip_prefix(text)?;
        Some(Self { addr, prefix })
    }

    /// Parse a `CIDR` literal. Host bits outside the mask must be zero.
    #[must_use]
    pub fn parse_cidr(text: &str) -> Option<Self> {
        let (addr, prefix) = parse_ip_prefix(text)?;
        if addr_bits(addr) != network_bits(addr, prefix) {
            return None;
        }
        Some(Self { addr, prefix })
    }

    /// Address family width in bits.
    #[must_use]
    pub const fn max_prefix(self) -> u8 {
        match self.addr {
            IpAddr::V4(_) => IPV4_BITS,
            IpAddr::V6(_) => IPV6_BITS,
        }
    }

    /// Raw IP address.
    #[must_use]
    pub const fn addr(self) -> IpAddr {
        self.addr
    }

    /// Network prefix length.
    #[must_use]
    pub const fn prefix(self) -> u8 {
        self.prefix
    }

    /// PostgreSQL `<<=` / `>>=` containment predicate.
    #[must_use]
    pub fn contains_or_equal(self, other: Self) -> bool {
        same_family(self.addr, other.addr)
            && self.prefix <= other.prefix
            && network_bits(self.addr, self.prefix) == network_bits(other.addr, self.prefix)
    }

    /// PostgreSQL `<<` / `>>` strict containment predicate.
    #[must_use]
    pub fn contains_strict(self, other: Self) -> bool {
        self.contains_or_equal(other)
            && !(self.prefix == other.prefix
                && network_bits(self.addr, self.prefix) == network_bits(other.addr, other.prefix))
    }

    /// PostgreSQL `&&` overlap predicate.
    #[must_use]
    pub fn overlaps(self, other: Self) -> bool {
        self.contains_or_equal(other) || other.contains_or_equal(self)
    }

    /// Add a signed offset to the address, preserving prefix length.
    #[must_use]
    pub fn checked_add(self, delta: i64) -> Option<Self> {
        let width = self.max_prefix();
        let bits = addr_bits(self.addr);
        let next = if delta >= 0 {
            bits.checked_add(u128::from(u64::try_from(delta).ok()?))?
        } else {
            bits.checked_sub(u128::from(delta.unsigned_abs()))?
        };
        if next > low_bits_mask(width) {
            return None;
        }
        Some(Self {
            addr: bits_to_addr(next, width)?,
            prefix: self.prefix,
        })
    }

    /// Difference between two same-family addresses.
    #[must_use]
    pub fn checked_sub_addr(self, other: Self) -> Option<i64> {
        if !same_family(self.addr, other.addr) {
            return None;
        }
        let left = i128::try_from(addr_bits(self.addr)).ok()?;
        let right = i128::try_from(addr_bits(other.addr)).ok()?;
        i64::try_from(left.checked_sub(right)?).ok()
    }

    /// Bitwise operation over same-family addresses.
    #[must_use]
    pub fn bitwise(self, other: Self, op: impl Fn(u128, u128) -> u128) -> Option<Self> {
        let width = self.max_prefix();
        if width != other.max_prefix() {
            return None;
        }
        let bits = op(addr_bits(self.addr), addr_bits(other.addr)) & low_bits_mask(width);
        Some(Self {
            addr: bits_to_addr(bits, width)?,
            prefix: self.prefix,
        })
    }

    /// Bitwise NOT, preserving prefix length.
    #[must_use]
    pub fn bit_not(self) -> Self {
        let width = self.max_prefix();
        let bits = !addr_bits(self.addr) & low_bits_mask(width);
        Self {
            addr: bits_to_addr(bits, width).expect("masked IP bits fit address family"),
            prefix: self.prefix,
        }
    }

    /// PostgreSQL binary payload for `inet` / `cidr`.
    #[must_use]
    pub fn to_pg_binary(self, cidr: bool) -> Vec<u8> {
        let mut out = Vec::with_capacity(20);
        match self.addr {
            IpAddr::V4(ip) => {
                out.extend_from_slice(&[PGSQL_AF_INET, self.prefix, u8::from(cidr), 4]);
                out.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                out.extend_from_slice(&[PGSQL_AF_INET6, self.prefix, u8::from(cidr), 16]);
                out.extend_from_slice(&ip.octets());
            }
        }
        out
    }

    /// Decode PostgreSQL binary `inet` / `cidr` payload.
    #[must_use]
    pub fn from_pg_binary(bytes: &[u8], cidr: bool) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let family = bytes[0];
        let prefix = bytes[1];
        let is_cidr = bytes[2] != 0;
        let len = usize::from(bytes[3]);
        if is_cidr != cidr || bytes.len() != 4 + len {
            return None;
        }
        let value = match (family, len) {
            (PGSQL_AF_INET, 4) => {
                let raw: [u8; 4] = bytes[4..8].try_into().ok()?;
                Self {
                    addr: IpAddr::V4(Ipv4Addr::from(raw)),
                    prefix,
                }
            }
            (PGSQL_AF_INET6, 16) => {
                let raw: [u8; 16] = bytes[4..20].try_into().ok()?;
                Self {
                    addr: IpAddr::V6(Ipv6Addr::from(raw)),
                    prefix,
                }
            }
            _ => return None,
        };
        if prefix > value.max_prefix() {
            return None;
        }
        if cidr && addr_bits(value.addr) != network_bits(value.addr, value.prefix) {
            return None;
        }
        Some(value)
    }

    fn cmp_key(self) -> (u8, u128, u8) {
        (family_rank(self.addr), addr_bits(self.addr), self.prefix)
    }
}

impl Ord for InetAddr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_key().cmp(&other.cmp_key())
    }
}

impl PartialOrd for InetAddr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// SQL `MACADDR` payload.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MacAddr {
    bytes: [u8; 6],
}

impl MacAddr {
    /// Parse a six-byte MAC address.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let bytes = parse_mac_bytes(text)?;
        Some(Self {
            bytes: bytes.try_into().ok()?,
        })
    }

    /// Octets in network order.
    #[must_use]
    pub const fn bytes(self) -> [u8; 6] {
        self.bytes
    }

    /// Bitwise operation over two `MACADDR` values.
    #[must_use]
    pub fn bitwise(self, other: Self, op: impl Fn(u8, u8) -> u8) -> Self {
        let mut bytes = [0_u8; 6];
        for (idx, out) in bytes.iter_mut().enumerate() {
            *out = op(self.bytes[idx], other.bytes[idx]);
        }
        Self { bytes }
    }

    /// Bitwise NOT.
    #[must_use]
    pub fn bit_not(self) -> Self {
        let mut bytes = [0_u8; 6];
        for (idx, out) in bytes.iter_mut().enumerate() {
            *out = !self.bytes[idx];
        }
        Self { bytes }
    }
}

/// SQL `MACADDR8` payload.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MacAddr8 {
    bytes: [u8; 8],
}

impl MacAddr8 {
    /// Parse an eight-byte MAC address, accepting six-byte input by
    /// inserting `ff:fe` in the middle like PostgreSQL.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let bytes = parse_mac_bytes(text)?;
        match bytes.len() {
            6 => Some(Self {
                bytes: [
                    bytes[0], bytes[1], bytes[2], 0xff, 0xfe, bytes[3], bytes[4], bytes[5],
                ],
            }),
            8 => Some(Self {
                bytes: bytes.try_into().ok()?,
            }),
            _ => None,
        }
    }

    /// Octets in network order.
    #[must_use]
    pub const fn bytes(self) -> [u8; 8] {
        self.bytes
    }

    /// Bitwise operation over two `MACADDR8` values.
    #[must_use]
    pub fn bitwise(self, other: Self, op: impl Fn(u8, u8) -> u8) -> Self {
        let mut bytes = [0_u8; 8];
        for (idx, out) in bytes.iter_mut().enumerate() {
            *out = op(self.bytes[idx], other.bytes[idx]);
        }
        Self { bytes }
    }

    /// Bitwise NOT.
    #[must_use]
    pub fn bit_not(self) -> Self {
        let mut bytes = [0_u8; 8];
        for (idx, out) in bytes.iter_mut().enumerate() {
            *out = !self.bytes[idx];
        }
        Self { bytes }
    }
}

/// Runtime value for PostgreSQL network address types.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetworkValue {
    /// `INET`.
    Inet(InetAddr),
    /// `CIDR`.
    Cidr(InetAddr),
    /// `MACADDR`.
    MacAddr(MacAddr),
    /// `MACADDR8`.
    MacAddr8(MacAddr8),
}

impl NetworkValue {
    /// Parse text for a target network SQL type.
    #[must_use]
    pub fn parse_for_type(target: &DataType, text: &str) -> Option<Self> {
        match target {
            DataType::Inet => InetAddr::parse_inet(text).map(Self::Inet),
            DataType::Cidr => InetAddr::parse_cidr(text).map(Self::Cidr),
            DataType::MacAddr => MacAddr::parse(text).map(Self::MacAddr),
            DataType::MacAddr8 => MacAddr8::parse(text).map(Self::MacAddr8),
            _ => None,
        }
    }

    /// Logical SQL type.
    #[must_use]
    pub const fn data_type(self) -> DataType {
        match self {
            Self::Inet(_) => DataType::Inet,
            Self::Cidr(_) => DataType::Cidr,
            Self::MacAddr(_) => DataType::MacAddr,
            Self::MacAddr8(_) => DataType::MacAddr8,
        }
    }

    /// IP address payload if this is `INET` or `CIDR`.
    #[must_use]
    pub const fn inet_addr(self) -> Option<InetAddr> {
        match self {
            Self::Inet(v) | Self::Cidr(v) => Some(v),
            Self::MacAddr(_) | Self::MacAddr8(_) => None,
        }
    }

    /// PostgreSQL binary wire/COPY payload.
    #[must_use]
    pub fn to_pg_binary(self) -> Vec<u8> {
        match self {
            Self::Inet(v) => v.to_pg_binary(false),
            Self::Cidr(v) => v.to_pg_binary(true),
            Self::MacAddr(v) => v.bytes().to_vec(),
            Self::MacAddr8(v) => v.bytes().to_vec(),
        }
    }

    /// Decode PostgreSQL binary wire/COPY payload.
    #[must_use]
    pub fn from_pg_binary(target: &DataType, bytes: &[u8]) -> Option<Self> {
        match target {
            DataType::Inet => InetAddr::from_pg_binary(bytes, false).map(Self::Inet),
            DataType::Cidr => InetAddr::from_pg_binary(bytes, true).map(Self::Cidr),
            DataType::MacAddr => Some(Self::MacAddr(MacAddr {
                bytes: bytes.try_into().ok()?,
            })),
            DataType::MacAddr8 => Some(Self::MacAddr8(MacAddr8 {
                bytes: bytes.try_into().ok()?,
            })),
            _ => None,
        }
    }

    /// Bitwise NOT for IP and MAC address families.
    #[must_use]
    pub fn bit_not(self) -> Self {
        match self {
            Self::Inet(v) | Self::Cidr(v) => Self::Inet(v.bit_not()),
            Self::MacAddr(v) => Self::MacAddr(v.bit_not()),
            Self::MacAddr8(v) => Self::MacAddr8(v.bit_not()),
        }
    }

    /// Bitwise operation for matching IP or MAC address families.
    #[must_use]
    pub fn bitwise(self, other: Self, op: impl Fn(u128, u128) -> u128) -> Option<Self> {
        match (self, other) {
            (Self::Inet(left) | Self::Cidr(left), Self::Inet(right) | Self::Cidr(right)) => {
                left.bitwise(right, op).map(Self::Inet)
            }
            (Self::MacAddr(left), Self::MacAddr(right)) => {
                Some(Self::MacAddr(left.bitwise(right, |a, b| {
                    u8::try_from(op(u128::from(a), u128::from(b)) & 0xff).expect("masked byte fits")
                })))
            }
            (Self::MacAddr8(left), Self::MacAddr8(right)) => {
                Some(Self::MacAddr8(left.bitwise(right, |a, b| {
                    u8::try_from(op(u128::from(a), u128::from(b)) & 0xff).expect("masked byte fits")
                })))
            }
            _ => None,
        }
    }

    /// Ordering key used by SQL comparison operators.
    #[must_use]
    pub fn cmp_network(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Inet(left) | Self::Cidr(left), Self::Inet(right) | Self::Cidr(right)) => {
                Some(left.cmp(&right))
            }
            (Self::MacAddr(left), Self::MacAddr(right)) => Some(left.cmp(&right)),
            (Self::MacAddr8(left), Self::MacAddr8(right)) => Some(left.cmp(&right)),
            _ => None,
        }
    }
}

impl fmt::Display for InetAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.prefix == self.max_prefix() {
            write!(f, "{}", self.addr)
        } else {
            write!(f, "{}/{}", self.addr, self.prefix)
        }
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_mac(f, &self.bytes)
    }
}

impl fmt::Display for MacAddr8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_mac(f, &self.bytes)
    }
}

impl fmt::Display for NetworkValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inet(v) => write!(f, "{v}"),
            Self::Cidr(v) => {
                if v.prefix() == v.max_prefix() {
                    write!(f, "{}/{}", v.addr(), v.prefix())
                } else {
                    write!(f, "{v}")
                }
            }
            Self::MacAddr(v) => write!(f, "{v}"),
            Self::MacAddr8(v) => write!(f, "{v}"),
        }
    }
}

fn parse_ip_prefix(text: &str) -> Option<(IpAddr, u8)> {
    let trimmed = text.trim();
    let (addr_text, prefix_text) = trimmed
        .split_once('/')
        .map_or((trimmed, None), |(addr, prefix)| (addr, Some(prefix)));
    let addr: IpAddr = addr_text.parse().ok()?;
    let max_prefix = match addr {
        IpAddr::V4(_) => IPV4_BITS,
        IpAddr::V6(_) => IPV6_BITS,
    };
    let prefix = match prefix_text {
        Some(prefix) => prefix.parse::<u8>().ok()?,
        None => max_prefix,
    };
    if prefix > max_prefix {
        return None;
    }
    Some((addr, prefix))
}

fn same_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

const fn family_rank(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 4,
        IpAddr::V6(_) => 6,
    }
}

fn addr_bits(addr: IpAddr) -> u128 {
    match addr {
        IpAddr::V4(ip) => u128::from(u32::from(ip)),
        IpAddr::V6(ip) => u128::from_be_bytes(ip.octets()),
    }
}

fn bits_to_addr(bits: u128, width: u8) -> Option<IpAddr> {
    match width {
        IPV4_BITS => Some(IpAddr::V4(Ipv4Addr::from(u32::try_from(bits).ok()?))),
        IPV6_BITS => Some(IpAddr::V6(Ipv6Addr::from(bits))),
        _ => None,
    }
}

fn network_bits(addr: IpAddr, prefix: u8) -> u128 {
    addr_bits(addr) & prefix_mask(prefix_width(addr), prefix)
}

const fn prefix_width(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => IPV4_BITS,
        IpAddr::V6(_) => IPV6_BITS,
    }
}

fn prefix_mask(width: u8, prefix: u8) -> u128 {
    if prefix == 0 {
        return 0;
    }
    let host_width = width - prefix;
    low_bits_mask(width) & !low_bits_mask(host_width)
}

fn low_bits_mask(width: u8) -> u128 {
    match width {
        0 => 0,
        128 => u128::MAX,
        other => (1_u128 << u32::from(other)) - 1,
    }
}

fn parse_mac_bytes(text: &str) -> Option<Vec<u8>> {
    let mut nibbles = Vec::with_capacity(16);
    for byte in text.trim().bytes() {
        match byte {
            b':' | b'-' | b'.' => {}
            _ => nibbles.push(hex_nibble(byte)?),
        }
    }
    if nibbles.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(nibbles.len() / 2);
    for pair in nibbles.chunks_exact(2) {
        bytes.push((pair[0] << 4) | pair[1]);
    }
    Some(bytes)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn write_mac(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for (idx, byte) in bytes.iter().enumerate() {
        if idx > 0 {
            f.write_str(":")?;
        }
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{InetAddr, NetworkValue};

    #[test]
    fn network_parse_display_and_containment() {
        let host = InetAddr::parse_inet("192.168.1.5/24").unwrap();
        let net = InetAddr::parse_cidr("192.168.1.0/24").unwrap();
        let wide = InetAddr::parse_inet("192.168.0.0/16").unwrap();
        assert_eq!(host.to_string(), "192.168.1.5/24");
        assert_eq!(NetworkValue::Cidr(net).to_string(), "192.168.1.0/24");
        assert!(wide.contains_strict(host));
        assert!(net.contains_or_equal(host));
        assert!(net.overlaps(InetAddr::parse_inet("192.168.1.128/25").unwrap()));
        assert!(InetAddr::parse_cidr("192.168.1.5/24").is_none());
    }

    #[test]
    fn network_arithmetic_and_mac_normalization() {
        let host = InetAddr::parse_inet("192.168.1.5/24").unwrap();
        assert_eq!(host.checked_add(5).unwrap().to_string(), "192.168.1.10/24");
        assert_eq!(host.checked_add(-4).unwrap().to_string(), "192.168.1.1/24");
        assert_eq!(
            host.checked_sub_addr(InetAddr::parse_inet("192.168.1.1").unwrap()),
            Some(4)
        );
        assert_eq!(
            NetworkValue::parse_for_type(&crate::DataType::MacAddr, "08-00-2B-01-02-03")
                .unwrap()
                .to_string(),
            "08:00:2b:01:02:03"
        );
        assert_eq!(
            NetworkValue::parse_for_type(&crate::DataType::MacAddr8, "08:00:2b:01:02:03")
                .unwrap()
                .to_string(),
            "08:00:2b:ff:fe:01:02:03"
        );
    }
}
