"""Map a latitude/longitude to an NYC borough name.

Returns one of: Manhattan, Brooklyn, Queens, Bronx, Staten Island, or
"Non-NYC" (for example the Jersey City and Hoboken stations).

Points that fall just outside a borough polygon (GPS noise, piers, docks on
the waterline) are assigned to the nearest borough when it is within
NEAR_METERS. Results are cached by coordinates rounded to four decimals
(about 11 m), so each distinct station location is resolved only once.
"""
from borough_data import BOROUGH_RINGS

NON_NYC = "Non-NYC"
NEAR_METERS = 150.0
_CACHE_LIMIT = 300000

# Bounding box per ring so most point-in-polygon tests exit early.
_RINGS = []
for _name, _rings in BOROUGH_RINGS.items():
    for _ring in _rings:
        _xs = [p[0] for p in _ring]
        _ys = [p[1] for p in _ring]
        _RINGS.append((_name, _ring, min(_xs), max(_xs), min(_ys), max(_ys)))

_cache = {}


def _inside(x, y, ring):
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y):
            if x < (xj - xi) * (y - yi) / (yj - yi) + xi:
                inside = not inside
        j = i
    return inside


def _dist_m(x, y, a, b):
    """Approximate distance in meters from point to segment a-b near NYC."""
    kx, ky = 84300.0, 111200.0  # meters per degree at about 40.7 N
    px, py = x * kx, y * ky
    ax, ay = a[0] * kx, a[1] * ky
    bx, by = b[0] * kx, b[1] * ky
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return ((px - ax) ** 2 + (py - ay) ** 2) ** 0.5
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    return ((px - (ax + t * dx)) ** 2 + (py - (ay + t * dy)) ** 2) ** 0.5


def _lookup(lat, lng):
    # Parity across all rings of a borough handles holes and islands.
    hits = {}
    for name, ring, x0, x1, y0, y1 in _RINGS:
        if x0 <= lng <= x1 and y0 <= lat <= y1 and _inside(lng, lat, ring):
            hits[name] = hits.get(name, 0) + 1
    for name, count in hits.items():
        if count % 2 == 1:
            return name
    best_name, best = None, NEAR_METERS
    for name, ring, x0, x1, y0, y1 in _RINGS:
        pad = 0.003
        if not (x0 - pad <= lng <= x1 + pad and y0 - pad <= lat <= y1 + pad):
            continue
        n = len(ring)
        for i in range(n):
            d = _dist_m(lng, lat, ring[i], ring[(i + 1) % n])
            if d < best:
                best, best_name = d, name
    return best_name or NON_NYC


def borough_of(lat, lng):
    key = (round(lat, 4), round(lng, 4))
    hit = _cache.get(key)
    if hit is None:
        if len(_cache) >= _CACHE_LIMIT:
            _cache.clear()
        hit = _lookup(key[0], key[1])
        _cache[key] = hit
    return hit
