import asyncio
import dns.asyncresolver
import dns.exception

# Use public resolvers — avoids relying on the container's /etc/resolv.conf
_resolver = dns.asyncresolver.Resolver()
_resolver.nameservers = ["8.8.8.8", "1.1.1.1"]
_resolver.timeout = 3
_resolver.lifetime = 5


async def resolve(hostname: str) -> str | None:
    """Return the first A record for hostname, or None on NXDOMAIN / timeout.

    None means the domain is dead — callers should treat it as PERMANENT_FAIL
    without attempting an HTTP connection.
    """
    try:
        answer = await _resolver.resolve(hostname, "A")
        return str(answer[0])
    except (dns.exception.DNSException, OSError):
        return None
