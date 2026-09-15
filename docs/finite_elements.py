"""
finite_elements.py
==================

Axisymmetric finite-element conduction solver for IN-CONTAINER thermal
processing — the heat-penetration and lethality arithmetic behind the
Process Authority note, expressed as code instead of as an assertion.

This is a HELPER module, in the same family as process_authority.py and
pathogen_italic.py: a pure library with no CLI, no main(), no I/O and no
network. It reads nothing and writes nothing. Import it, call simulate(),
read the numbers. Nothing in the scheduled pipeline depends on it.

WHY IT EXISTS
-------------
The weekly and monthly briefings claim process authority over thermal
processing ("interpretation under engineering authority"). When a recall
window fires the Process Authority trigger — a botulinum signal, a
low-acid canned food, a vacuum-packed RTE — the engineering question
underneath is always the same shape: given this container, this product
and this retort schedule, where is the cold spot and how much lethality
does it actually accumulate? This module answers that question from
first principles so the answer is reproducible rather than remembered.

PUBLICATION BOUNDARY — read before wiring this into a report builder
--------------------------------------------------------------------
process_authority.py rule 4 forbids publishing a specific F-value,
D-value, z-value or temperature target in briefing text, because those
are engagement deliverables from a qualified process authority and are
meaningless without per-product validation. This module produces exactly
those numbers. They are INTERNAL: analysis, sanity-checking, teaching,
and engagement work — never the public paragraph. summarize() carries
that banner in its own output so a number lifted out of a log carries
its own provenance. The thermophysical constants in PRODUCTS are
textbook representative values, NOT validated product data, and a
simulated F0 is not a scheduled process.

THE MODEL
---------
Transient conduction in a finite cylinder, axisymmetric in (r, z):

    rho*cp dT/dt = (1/r) d/dr ( k r dT/dr ) + d/dz ( k dT/dz )

solved by Galerkin FEM with bilinear quadrilateral elements on a
structured (r, z) mesh, 2x2 Gauss quadrature, and the axisymmetric
volume weight 2*pi*r. Boundary conditions:

  * r = 0          symmetry — natural in the axisymmetric weak form, the
                   2*pi*r weight vanishes there, so nothing is imposed
  * r = R, z = 0, z = H
                   convective (Robin): q = h (T_medium(t) - T_surface).
                   h=None models the isothermal surface of a condensing
                   steam retort via a scaled penalty (see _penalty_h).

Time integration is the generalised theta-method (theta=1 backward
Euler by default — unconditionally stable and monotone, which matters
because a come-up ramp followed by a cooling step is a stiff problem
with sharp corners). The system matrix is symmetric positive definite
and constant in time, so it is Cholesky-factorised ONCE in banded
storage and back-substituted per step. That keeps a 1-hour process on a
200-node mesh inside a few seconds of pure-stdlib Python — no numpy, no
scipy, no new dependency in requirements.txt.

Lethality is accumulated AT EVERY NODE, by trapezoid, on the reference
lethal rate L = 10**((T - Tref)/z). Mass-average lethality is then the
volume-weighted mean of the nodal F-values — never F of the average
temperature, which is a different and wrong number, because L is
convex in T.

VERIFICATION
------------
tests/test_finite_elements.py checks the solver against the closed-form
product solution for a finite cylinder with an isothermal surface (the
Bessel series in r times the cosine series in z), against Ball's f_h and
j_h for the same geometry, and against the discrete invariants that
catch an assembly bug: the capacity matrix must integrate to rho*cp*V,
and the conduction matrix must annihilate a uniform temperature field.

Public API
----------
    Container, Product, RetortStep, RetortSchedule, SolveOptions
    CONTAINERS, PRODUCTS            — representative starting points
    simulate(...)  -> SimulationResult
    lethal_rate(), f_value()        — the lethality primitives
    analytic_f_h_s()                — closed-form f_h, for cross-checking
    oscillation_free_time_step_s()  — the consistent-capacity dt limit
    summarize(result)               — internal-use text block
    temperature_map(result)         — ASCII cross-section of the field

Usage
-----
There is no CLI and no main() on purpose — this is a library, and the
repository already has enough entry points. From the repo root:

    import sys; sys.path.insert(0, "docs")
    import finite_elements as fe

    result = fe.simulate(
        fe.CONTAINERS["307x409"],
        fe.PRODUCTS["vegetable_puree"],
        fe.RetortSchedule.standard(start_c=70.0, come_up_s=600.0,
                                   hold_c=121.11, hold_s=4500.0,
                                   cool_c=40.0, cool_s=900.0),
        initial_c=70.0)

    print(fe.summarize(result))
    result.f0_cold_spot_min        # 4.62 min
    result.f0_mass_avg_min         # 31.48 min — the conduction gap
    result.heat_penetration.f_h_min

That run takes about 0.7 s for a 100-minute process on the default
10x16 mesh. The gap between those two F0 numbers is the whole reason
the cold spot is the only one a process is judged on.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

__all__ = [
    "F0_REF_C", "F0_Z_C", "COOK_REF_C", "COOK_Z_C",
    "Container", "Product", "RetortStep", "RetortSchedule", "SolveOptions",
    "CONTAINERS", "PRODUCTS",
    "HeatPenetration", "SimulationResult",
    "lethal_rate", "f_value", "analytic_f_h_s",
    "oscillation_free_time_step_s",
    "simulate", "summarize", "temperature_map",
    "BALL_J_FINITE_CYLINDER", "INTERNAL_BANNER",
]

# ──────────────────────────────────────────────────────────────────────
# Reference kinetics
# ──────────────────────────────────────────────────────────────────────
# F0 — the botulinum cook. 121.11 C is 250 F exactly; z = 10 C is the
# conventional value for Clostridium botulinum spores in low-acid foods.
F0_REF_C = 121.11
F0_Z_C = 10.0

# Cook value C0 — quality degradation, not safety. 100 C / z = 33.1 C is
# the conventional thiamine-loss / overall-quality pairing. Reported
# alongside F0 because every extra minute of safety is paid for here.
COOK_REF_C = 100.0
COOK_Z_C = 33.1

_LN10 = math.log(10.0)

# Ratio of the first-term centre coefficients used by Ball's method:
# 1.6021 (infinite cylinder) * 1.2732 (infinite slab) = 2.0396.
BALL_J_FINITE_CYLINDER = 2.0396

# First root of J0 — the radial eigenvalue of an isothermal-surface
# cylinder, and the analytic f_h that HeatPenetration is checked against.
_BESSEL_J0_ROOT_1 = 2.404825557695773

# Semi-log fit windows, tried in order: (g_high, g_low, minimum points).
# The primary window is the asymptotic stretch of the heating curve. The
# fallback exists so a short process still reports something, flagged
# through HeatPenetration.g_window rather than silently.
_FIT_WINDOWS = ((0.30, 0.02, 5), (0.60, 0.02, 5), (0.80, 0.02, 3))


# ──────────────────────────────────────────────────────────────────────
# Lethality primitives
# ──────────────────────────────────────────────────────────────────────
def lethal_rate(temp_c: float,
                ref_c: float = F0_REF_C,
                z_c: float = F0_Z_C) -> float:
    """Instantaneous lethal rate L = 10**((T - Tref)/z).

    Dimensionless: minutes of reference lethality per minute of process
    time. At T = Tref it is exactly 1.0; ten degrees below (z=10) it is
    0.1, which is why the come-up contributes so little and the cooling
    curve contributes more than operators expect.
    """
    return math.exp(_LN10 * (temp_c - ref_c) / z_c)


def f_value(times_s: Sequence[float],
            temps_c: Sequence[float],
            ref_c: float = F0_REF_C,
            z_c: float = F0_Z_C) -> float:
    """Trapezoidal integral of the lethal rate over a temperature history.

    times_s in SECONDS, result in MINUTES — the units the industry
    quotes F0 in. Sequences must be the same length and time-ordered.
    """
    if len(times_s) != len(temps_c):
        raise ValueError("times_s and temps_c must be the same length")
    if len(times_s) < 2:
        return 0.0
    total = 0.0
    prev_rate = lethal_rate(temps_c[0], ref_c, z_c)
    for idx in range(1, len(times_s)):
        rate = lethal_rate(temps_c[idx], ref_c, z_c)
        total += 0.5 * (prev_rate + rate) * (times_s[idx] - times_s[idx - 1])
        prev_rate = rate
    return total / 60.0


def oscillation_free_time_step_s(container: "Container",
                                 product: "Product",
                                 options: "SolveOptions") -> float:
    """Smallest time step the CONSISTENT capacity matrix integrates
    without spatial overshoot: dt >= h_min**2 / (6 alpha).

    The classical criterion for a consistent mass matrix with backward
    Euler on a parabolic problem. Below it the solution can overshoot
    the boundary temperature by a fraction of a degree at nodes next to
    a corner while a steep front is passing — a discretisation
    artefact, not heat. On a 307x409 of puree at a 10x16 mesh the
    criterion is about 23 s, and the measured overshoot is +0.34 C at
    dt=5 s, +0.13 C at dt=20 s and exactly zero by dt=30 s.

    It is reported rather than enforced, because meeting it means a time
    step far coarser than lethality integration wants. The defensible
    position is the one taken here: the artefact is bounded, it sits at
    the hot surface and never at the cold spot, the cold-spot F0 it
    perturbs is the fourth decimal place — and SolveOptions(
    lumped_capacity=True) removes it entirely for anyone who would
    rather have monotonicity than the last percent of accuracy.
    """
    h_min = min(container.radius_m / options.radial_divisions,
                container.height_m / options.axial_divisions_even)
    return h_min * h_min / (6.0 * product.diffusivity_m2_s)


def analytic_f_h_s(container: "Container", product: "Product") -> float:
    """Asymptotic f_h for an isothermal-surface finite cylinder, seconds.

    The closed form behind Ball's f_h = 0.398 / (alpha (1/R**2 + ...)):
    the first eigenvalue of the finite cylinder is the sum of the radial
    and axial ones, and f_h is one log cycle of that decay,

        lambda_1 = alpha ( (2.4048/R)**2 + (pi/H)**2 )
        f_h      = ln(10) / lambda_1

    This is what simulate() must reproduce on a long process with a step
    change at the wall, and it is the cheapest possible check that a
    geometry and a diffusivity have been entered in the units they claim.
    It says nothing about a real process: it assumes an instantaneous
    come-up and a surface pinned at the medium temperature.
    """
    lam = product.diffusivity_m2_s * (
        (_BESSEL_J0_ROOT_1 / container.radius_m) ** 2
        + (math.pi / container.height_m) ** 2)
    return _LN10 / lam


# ──────────────────────────────────────────────────────────────────────
# Geometry
# ──────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Container:
    """A right-circular cylinder: can, jar, or a bowl treated as one.

    Dimensions are the PRODUCT cavity, not the shipping dimensions. The
    difference is a few millimetres of headspace and seam, which moves
    f_h by more than most people assume — the axial term goes as 1/H**2.
    """
    name: str
    diameter_m: float
    height_m: float

    @property
    def radius_m(self) -> float:
        return self.diameter_m / 2.0

    @property
    def volume_m3(self) -> float:
        return math.pi * self.radius_m ** 2 * self.height_m

    @classmethod
    def from_can_code(cls, code: str, name: Optional[str] = None
                      ) -> "Container":
        """Build from an American can code, e.g. '307x409'.

        Each half is three digits: whole inches, then sixteenths. 307 is
        3 + 7/16 in = 87.3 mm; 409 is 4 + 9/16 in = 115.9 mm. The code
        describes the outside of the double seam, so this is a nominal
        cavity, generous by a millimetre or two.
        """
        parts = code.lower().replace("-", "x").split("x")
        if len(parts) != 2 or not all(p.isdigit() and len(p) == 3
                                      for p in parts):
            raise ValueError(
                f"can code must be two 3-digit halves, e.g. '307x409'; "
                f"got {code!r}")
        dims = [(int(p[0]) + int(p[1:]) / 16.0) * 0.0254 for p in parts]
        return cls(name=name or code, diameter_m=dims[0], height_m=dims[1])


@dataclass(frozen=True)
class Product:
    """Thermophysical properties of a conduction-heating food.

    REPRESENTATIVE VALUES ONLY. Real products are measured, and the
    measurement is part of the process-authority engagement. A 10% error
    in diffusivity is a 10% error in f_h, which at the end of a long
    conduction cook is a large error in F0.
    """
    name: str
    conductivity_w_mk: float
    density_kg_m3: float
    specific_heat_j_kgk: float

    @property
    def diffusivity_m2_s(self) -> float:
        return (self.conductivity_w_mk
                / (self.density_kg_m3 * self.specific_heat_j_kgk))


# Starting points, not data. Sources are the usual handbook ranges for
# conduction-heating products; every one of them must be replaced by a
# measured value before it means anything about a real process.
PRODUCTS: Dict[str, Product] = {
    "vegetable_puree": Product("Vegetable puree", 0.55, 1080.0, 3700.0),
    "meat_paste": Product("Meat paste / pate", 0.45, 1050.0, 3500.0),
    "starch_gravy": Product("Starch-thickened gravy", 0.50, 1050.0, 3600.0),
    "brine": Product("Brine / water-like pack", 0.62, 1000.0, 4180.0),
}

CONTAINERS: Dict[str, Container] = {
    "211x400": Container.from_can_code("211x400", "211x400 tall"),
    "307x409": Container.from_can_code("307x409", "307x409 (No. 300)"),
    "603x700": Container.from_can_code("603x700", "603x700 (A10)"),
}


# ──────────────────────────────────────────────────────────────────────
# Process schedule
# ──────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RetortStep:
    """One leg of the medium-temperature profile.

    target_c is the temperature at the END of the step. ramp=True walks
    linearly there from wherever the previous step finished (a come-up or
    a cool-down); ramp=False jumps to target_c immediately and holds it
    (a hold leg, or the idealised step change used in verification).
    """
    duration_s: float
    target_c: float
    ramp: bool = True


@dataclass(frozen=True)
class RetortSchedule:
    """Heating-medium temperature as a function of time."""
    start_c: float
    steps: Tuple[RetortStep, ...]

    @property
    def total_s(self) -> float:
        return sum(s.duration_s for s in self.steps)

    def temperature_at(self, t_s: float) -> float:
        """Medium temperature at time t. Clamped at both ends."""
        if t_s <= 0.0:
            return self.start_c
        prev = self.start_c
        elapsed = 0.0
        for step in self.steps:
            if t_s <= elapsed + step.duration_s:
                if not step.ramp or step.duration_s <= 0.0:
                    return step.target_c
                frac = (t_s - elapsed) / step.duration_s
                return prev + (step.target_c - prev) * frac
            elapsed += step.duration_s
            prev = step.target_c
        return prev

    @classmethod
    def standard(cls, *,
                 start_c: float,
                 come_up_s: float,
                 hold_c: float,
                 hold_s: float,
                 cool_c: float = 40.0,
                 cool_s: float = 900.0) -> "RetortSchedule":
        """The ordinary three-leg retort cycle: come-up, hold, cool.

        The come-up is a ramp because a retort is not a step function,
        and the difference shows up in F0 — Ball's 0.42 correction exists
        precisely because the come-up is neither free nor fully lethal.
        """
        return cls(start_c=start_c, steps=(
            RetortStep(come_up_s, hold_c, ramp=True),
            RetortStep(hold_s, hold_c, ramp=False),
            RetortStep(cool_s, cool_c, ramp=True),
        ))


@dataclass(frozen=True)
class SolveOptions:
    """Discretisation and boundary settings.

    radial_divisions / axial_divisions are ELEMENT counts; the node grid
    is one larger in each direction. axial_divisions is forced even so a
    node lands exactly on the geometric centre — the cold spot of a
    conduction pack is the one number nobody should be interpolating.

    surface_h_w_m2k=None models a condensing-steam retort: the surface
    follows the medium exactly. Give a number to model a real coefficient
    (steam/air mixtures and water cascade run roughly 200-2000 W/m2K).

    theta=1 is backward Euler: first-order in time, unconditionally
    stable, and monotone in time. theta=0.5 is Crank-Nicolson, second
    order, but it rings on the sharp corner where a come-up meets a
    hold — which is exactly where a lethality integral is most sensitive.
    Backward Euler with a small step is the safer trade.

    lumped_capacity=False (the default) keeps the consistent capacity
    matrix, which is the more accurate of the two on the cold-spot
    history — the number that governs safety. Its cost is a bounded
    overshoot at the hot surface when the time step is below
    oscillation_free_time_step_s(); set lumped_capacity=True to trade
    that last percent of accuracy for strict monotonicity.
    """
    radial_divisions: int = 10
    axial_divisions: int = 16
    time_step_s: float = 5.0
    surface_h_w_m2k: Optional[float] = None
    theta: float = 1.0
    lumped_capacity: bool = False

    def __post_init__(self) -> None:
        if self.radial_divisions < 2 or self.axial_divisions < 2:
            raise ValueError("need at least 2 divisions in each direction")
        if self.time_step_s <= 0.0:
            raise ValueError("time_step_s must be positive")
        if not 0.0 <= self.theta <= 1.0:
            raise ValueError("theta must lie in [0, 1]")

    @property
    def axial_divisions_even(self) -> int:
        return self.axial_divisions + (self.axial_divisions % 2)


# ──────────────────────────────────────────────────────────────────────
# Symmetric banded matrix — upper band storage, band[i][d] = A[i][i+d]
# ──────────────────────────────────────────────────────────────────────
# The mesh is numbered radius-fastest, so a node couples only to nodes
# within one mesh row: half-bandwidth = (nr+1)+1. Storing the full
# (n x n) matrix would be 30x the memory and Cholesky would be O(n**3)
# instead of O(n*bw**2) — the difference between a test suite that runs
# and one that doesn't.
# ──────────────────────────────────────────────────────────────────────
class _Band:
    __slots__ = ("n", "bw", "a", "_u")

    def __init__(self, n: int, bw: int) -> None:
        self.n = n
        self.bw = bw
        self.a = [[0.0] * (bw + 1) for _ in range(n)]
        self._u: Optional[List[List[float]]] = None

    def add(self, i: int, j: int, value: float) -> None:
        if j < i:
            i, j = j, i
        self.a[i][j - i] += value

    def get(self, i: int, j: int) -> float:
        if j < i:
            i, j = j, i
        d = j - i
        return self.a[i][d] if d <= self.bw else 0.0

    def row_sums(self) -> List[float]:
        """Sum of each full (symmetric) row. For a capacity matrix this
        is the lumped nodal mass*cp, and summing it gives rho*cp*V."""
        out = [0.0] * self.n
        for i in range(self.n):
            row = self.a[i]
            out[i] += row[0]
            for d in range(1, self.bw + 1):
                j = i + d
                if j >= self.n:
                    break
                v = row[d]
                if v:
                    out[i] += v
                    out[j] += v
        return out

    def lump(self) -> "_Band":
        """Row-sum lumped copy: diagonal, same total."""
        out = _Band(self.n, self.bw)
        for i, s in enumerate(self.row_sums()):
            out.a[i][0] = s
        return out

    def matvec(self, x: Sequence[float]) -> List[float]:
        y = [0.0] * self.n
        for i in range(self.n):
            row = self.a[i]
            xi = x[i]
            y[i] += row[0] * xi
            for d in range(1, self.bw + 1):
                j = i + d
                if j >= self.n:
                    break
                v = row[d]
                if v:
                    y[i] += v * x[j]
                    y[j] += v * xi
        return y

    def combine(self, other: "_Band", sa: float, sb: float) -> "_Band":
        """sa*self + sb*other, same sparsity pattern."""
        out = _Band(self.n, self.bw)
        for i in range(self.n):
            ra, rb, ro = self.a[i], other.a[i], out.a[i]
            for d in range(self.bw + 1):
                ro[d] = sa * ra[d] + sb * rb[d]
        return out

    def factorise(self) -> None:
        """Banded Cholesky, A = U^T U, in place on a private copy."""
        n, bw = self.n, self.bw
        u = [row[:] for row in self.a]
        for i in range(n):
            for j in range(i, min(i + bw, n - 1) + 1):
                s = u[i][j - i]
                kmin = max(0, i - bw)
                for k in range(kmin, i):
                    dk_i = i - k
                    dk_j = j - k
                    if dk_j <= bw:
                        s -= u[k][dk_i] * u[k][dk_j]
                if j == i:
                    if s <= 0.0:
                        raise ValueError(
                            "system matrix is not positive definite — "
                            "check mesh, time step and material properties")
                    u[i][0] = math.sqrt(s)
                else:
                    u[i][j - i] = s / u[i][0]
        self._u = u

    def solve(self, b: Sequence[float]) -> List[float]:
        if self._u is None:
            self.factorise()
        u = self._u
        n, bw = self.n, self.bw
        y = [0.0] * n
        for i in range(n):
            s = b[i]
            for k in range(max(0, i - bw), i):
                s -= u[k][i - k] * y[k]
            y[i] = s / u[i][0]
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            s = y[i]
            row = u[i]
            for d in range(1, bw + 1):
                j = i + d
                if j >= n:
                    break
                s -= row[d] * x[j]
            x[i] = s / row[0]
        return x


# ──────────────────────────────────────────────────────────────────────
# Mesh + assembly
# ──────────────────────────────────────────────────────────────────────
_G = 1.0 / math.sqrt(3.0)
_GAUSS_2 = (-_G, _G)


def _shape(xi: float, eta: float
           ) -> Tuple[List[float], List[float], List[float]]:
    """Bilinear shapes and their derivatives in the reference square."""
    n = [0.25 * (1 - xi) * (1 - eta), 0.25 * (1 + xi) * (1 - eta),
         0.25 * (1 + xi) * (1 + eta), 0.25 * (1 - xi) * (1 + eta)]
    dxi = [-0.25 * (1 - eta), 0.25 * (1 - eta),
           0.25 * (1 + eta), -0.25 * (1 + eta)]
    deta = [-0.25 * (1 - xi), -0.25 * (1 + xi),
            0.25 * (1 + xi), 0.25 * (1 - xi)]
    return n, dxi, deta


class _System:
    """Assembled, geometry-only pieces of the discrete problem."""

    def __init__(self, r_nodes: List[float], z_nodes: List[float],
                 capacity: _Band, conduction: _Band, surface: _Band,
                 surface_load: List[float]) -> None:
        self.r_nodes = r_nodes
        self.z_nodes = z_nodes
        self.capacity = capacity          # rho*cp integrated
        self.conduction = conduction      # k integrated
        self.surface = surface            # per unit h
        self.surface_load = surface_load  # integral of Na over the wetted area
        self.n_nodes = len(r_nodes) * len(z_nodes)

    def index(self, i: int, j: int) -> int:
        return j * len(self.r_nodes) + i


def _assemble(container: Container, product: Product,
              options: SolveOptions) -> _System:
    """Build C, K, the surface operator and the surface load vector.

    Everything carries the axisymmetric weight 2*pi*r, which is why the
    symmetry axis needs no special case: the weight is zero there.
    """
    nr = options.radial_divisions
    nz = options.axial_divisions_even
    r_nodes = [container.radius_m * i / nr for i in range(nr + 1)]
    z_nodes = [container.height_m * j / nz for j in range(nz + 1)]
    n_r = nr + 1
    n_nodes = n_r * (nz + 1)
    bw = n_r + 1

    cap = _Band(n_nodes, bw)
    con = _Band(n_nodes, bw)
    srf = _Band(n_nodes, bw)
    load = [0.0] * n_nodes

    k = product.conductivity_w_mk
    rho_cp = product.density_kg_m3 * product.specific_heat_j_kgk

    def idx(i: int, j: int) -> int:
        return j * n_r + i

    # ---- volume terms -------------------------------------------------
    for j in range(nz):
        dz = z_nodes[j + 1] - z_nodes[j]
        for i in range(nr):
            dr = r_nodes[i + 1] - r_nodes[i]
            nodes = (idx(i, j), idx(i + 1, j),
                     idx(i + 1, j + 1), idx(i, j + 1))
            det_j = dr * dz / 4.0
            for xi in _GAUSS_2:
                rr = r_nodes[i] + dr * (1.0 + xi) / 2.0
                for eta in _GAUSS_2:
                    n_sh, dxi, deta = _shape(xi, eta)
                    dndr = [d * 2.0 / dr for d in dxi]
                    dndz = [d * 2.0 / dz for d in deta]
                    coef = 2.0 * math.pi * rr * det_j     # Gauss weight = 1
                    for a in range(4):
                        for b in range(a, 4):
                            con.add(nodes[a], nodes[b],
                                    k * (dndr[a] * dndr[b]
                                         + dndz[a] * dndz[b]) * coef)
                            cap.add(nodes[a], nodes[b],
                                    rho_cp * n_sh[a] * n_sh[b] * coef)

    # ---- wetted surface: side wall + both ends ------------------------
    def edge(node_a: int, node_b: int,
             r_a: float, r_b: float, z_a: float, z_b: float) -> None:
        length = math.hypot(r_b - r_a, z_b - z_a)
        for s in _GAUSS_2:
            n_sh = ((1.0 - s) / 2.0, (1.0 + s) / 2.0)
            rr = r_a + (r_b - r_a) * (1.0 + s) / 2.0
            coef = 2.0 * math.pi * rr * length / 2.0
            pair = (node_a, node_b)
            for a in range(2):
                load[pair[a]] += n_sh[a] * coef
                for b in range(a, 2):
                    srf.add(pair[a], pair[b], n_sh[a] * n_sh[b] * coef)

    for j in range(nz):                                  # side wall r = R
        edge(idx(nr, j), idx(nr, j + 1),
             r_nodes[nr], r_nodes[nr], z_nodes[j], z_nodes[j + 1])
    for i in range(nr):                                  # both flat ends
        edge(idx(i, 0), idx(i + 1, 0),
             r_nodes[i], r_nodes[i + 1], z_nodes[0], z_nodes[0])
        edge(idx(i, nz), idx(i + 1, nz),
             r_nodes[i], r_nodes[i + 1], z_nodes[nz], z_nodes[nz])

    if options.lumped_capacity:
        cap = cap.lump()
    return _System(r_nodes, z_nodes, cap, con, srf, load)


def _penalty_h(product: Product, container: Container,
               options: SolveOptions) -> float:
    """Surface coefficient that stands in for an isothermal boundary.

    Scaled off k/h_min so the penalty is a fixed ~1e-6 relative error in
    the surface temperature regardless of mesh or material, rather than a
    hard-coded 1e8 that is either too weak on a coarse mesh or wrecks the
    conditioning on a fine one.
    """
    h_min = min(container.radius_m / options.radial_divisions,
                container.height_m / options.axial_divisions_even)
    return 1.0e6 * product.conductivity_w_mk / h_min


# ──────────────────────────────────────────────────────────────────────
# Results
# ──────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class HeatPenetration:
    """Ball parameters recovered FROM the simulation, not assumed by it.

    f_h_s     — seconds for the straight-line portion of the semi-log
                heating curve to cross one log cycle
    j_h       — lag factor, the intercept of that line extrapolated back
                to the start of the constant-temperature hold, divided by
                the initial temperature difference
    r2        — fit quality; below ~0.9999 the window still holds
                curvature and the numbers should not be quoted
    g_window  — the (high, low) unaccomplished-difference bounds the fit
                actually used, so a widened fallback window is visible in
                the result rather than silent
    """
    f_h_s: float
    j_h: float
    r2: float
    window_s: Tuple[float, float]
    n_points: int
    g_window: Tuple[float, float]

    @property
    def f_h_min(self) -> float:
        return self.f_h_s / 60.0


@dataclass
class SimulationResult:
    container: Container
    product: Product
    schedule: RetortSchedule
    options: SolveOptions
    initial_c: float
    times_s: List[float]
    medium_c: List[float]
    center_c: List[float]
    mass_avg_c: List[float]
    f0_center_series_min: List[float]
    r_nodes: List[float]
    z_nodes: List[float]
    final_temps_c: List[float]
    peak_temps_c: List[float]
    trough_temps_c: List[float]
    f0_nodes_min: List[float]
    cook_nodes_min: List[float]
    mass_weights: List[float]
    heat_penetration: Optional[HeatPenetration]

    # -- headline numbers ----------------------------------------------
    @property
    def f0_cold_spot_min(self) -> float:
        """The number a scheduled process is judged on: least lethality
        anywhere in the container."""
        return min(self.f0_nodes_min)

    @property
    def f0_mass_avg_min(self) -> float:
        total = sum(self.mass_weights)
        return sum(m * f for m, f in
                   zip(self.mass_weights, self.f0_nodes_min)) / total

    @property
    def cook_mass_avg_min(self) -> float:
        total = sum(self.mass_weights)
        return sum(m * c for m, c in
                   zip(self.mass_weights, self.cook_nodes_min)) / total

    @property
    def cook_surface_min(self) -> float:
        return max(self.cook_nodes_min)

    @property
    def peak_center_c(self) -> float:
        return max(self.center_c)

    @property
    def cold_spot_rz(self) -> Tuple[float, float]:
        """(r, z) of the least-lethality node, in metres."""
        n_r = len(self.r_nodes)
        node = min(range(len(self.f0_nodes_min)),
                   key=lambda i: self.f0_nodes_min[i])
        return self.r_nodes[node % n_r], self.z_nodes[node // n_r]

    @property
    def total_time_s(self) -> float:
        return self.times_s[-1]


# ──────────────────────────────────────────────────────────────────────
# Solver
# ──────────────────────────────────────────────────────────────────────
def simulate(container: Container,
             product: Product,
             schedule: RetortSchedule,
             *,
             initial_c: float,
             options: Optional[SolveOptions] = None) -> SimulationResult:
    """Run the process and return everything it produced.

    initial_c is the uniform product temperature at t=0 — the filling /
    initial temperature, which is a scheduled-process parameter in its
    own right and is why a cold fill is not a free variable.
    """
    opts = options or SolveOptions()
    sys_ = _assemble(container, product, opts)
    n = sys_.n_nodes
    dt = opts.time_step_s
    theta = opts.theta

    h = (opts.surface_h_w_m2k if opts.surface_h_w_m2k is not None
         else _penalty_h(product, container, opts))
    if h <= 0.0:
        raise ValueError("surface_h_w_m2k must be positive")

    # K_total = conduction + h * surface;  f(t) = h * T_medium(t) * load
    k_total = sys_.conduction.combine(sys_.surface, 1.0, h)
    cap = sys_.capacity

    lhs = cap.combine(k_total, 1.0 / dt, theta)
    rhs_op = cap.combine(k_total, 1.0 / dt, -(1.0 - theta))
    lhs.factorise()

    mass_weights = cap.row_sums()
    total_mass = sum(mass_weights)

    center = sys_.index(0, opts.axial_divisions_even // 2)

    temps = [initial_c] * n
    peak = temps[:]
    trough = temps[:]
    f0_nodes = [0.0] * n
    cook_nodes = [0.0] * n
    rate_f0 = [lethal_rate(initial_c, F0_REF_C, F0_Z_C)] * n
    rate_ck = [lethal_rate(initial_c, COOK_REF_C, COOK_Z_C)] * n

    t = 0.0
    total_s = schedule.total_s
    n_steps = max(1, int(round(total_s / dt)))

    times = [0.0]
    medium = [schedule.temperature_at(0.0)]
    center_series = [initial_c]
    mass_series = [initial_c]
    f0_series = [0.0]

    dt_min = dt / 60.0
    load = sys_.surface_load
    for _ in range(n_steps):
        t_next = t + dt
        f_old = h * schedule.temperature_at(t)
        f_new = h * schedule.temperature_at(t_next)
        base = rhs_op.matvec(temps)
        scale = theta * f_new + (1.0 - theta) * f_old
        rhs = [base[i] + scale * load[i] for i in range(n)]
        temps = lhs.solve(rhs)

        # Nodal lethality, trapezoid on the rate. Done per node because
        # L is convex in T: the mass average of F is not F of the mass
        # average, and the gap is the whole quality argument.
        for i in range(n):
            ti = temps[i]
            if ti > peak[i]:
                peak[i] = ti
            elif ti < trough[i]:
                # Tracked because an undershoot below the coldest medium
                # temperature is the signature of a ringing time
                # integrator, not of anything physical.
                trough[i] = ti
            r_f0 = lethal_rate(ti, F0_REF_C, F0_Z_C)
            r_ck = lethal_rate(ti, COOK_REF_C, COOK_Z_C)
            f0_nodes[i] += 0.5 * (rate_f0[i] + r_f0) * dt_min
            cook_nodes[i] += 0.5 * (rate_ck[i] + r_ck) * dt_min
            rate_f0[i] = r_f0
            rate_ck[i] = r_ck

        t = t_next
        times.append(t)
        medium.append(schedule.temperature_at(t))
        center_series.append(temps[center])
        mass_series.append(
            sum(m * x for m, x in zip(mass_weights, temps)) / total_mass)
        f0_series.append(f0_nodes[center])

    hp = _fit_heat_penetration(times, medium, center_series, initial_c,
                               schedule)
    return SimulationResult(
        container=container, product=product, schedule=schedule,
        options=opts, initial_c=initial_c,
        times_s=times, medium_c=medium, center_c=center_series,
        mass_avg_c=mass_series, f0_center_series_min=f0_series,
        r_nodes=sys_.r_nodes, z_nodes=sys_.z_nodes,
        final_temps_c=temps, peak_temps_c=peak, trough_temps_c=trough,
        f0_nodes_min=f0_nodes, cook_nodes_min=cook_nodes,
        mass_weights=mass_weights, heat_penetration=hp)


def _fit_heat_penetration(times: Sequence[float],
                          medium: Sequence[float],
                          center: Sequence[float],
                          initial_c: float,
                          schedule: RetortSchedule
                          ) -> Optional[HeatPenetration]:
    """Recover f_h and j_h from the simulated centre curve.

    The fit runs over the constant-temperature hold, restricted to the
    part where the unaccomplished temperature difference g has fallen
    between 0.30 and 0.02 of its initial value — after the higher modes
    have died and before the curve flattens into rounding noise. The
    upper bound matters: fit from g=0.5 and the curve is still
    approaching its asymptote from below, which flattens the slope and
    inflates f_h by around 3% on a normal can. A short process that
    cannot fill the primary window falls back to a wider one, and
    HeatPenetration.g_window records which was used.

    j_h is referenced to the START OF THE HOLD, so it is the textbook
    2.04 when the come-up is instantaneous and drifts below it when it
    is not. Ball's 0.42*come-up correction is deliberately NOT applied:
    this reports what the simulation did, not what Ball's method would
    have assumed.
    """
    hold_t = 0.0
    hold_c = None
    elapsed = 0.0
    for step in schedule.steps:
        if not step.ramp:
            hold_t, hold_c = elapsed, step.target_c
            break
        elapsed += step.duration_s
    if hold_c is None:                       # no constant-temperature leg
        return None

    span = hold_c - initial_c
    if span <= 0.0:
        return None

    for g_hi, g_lo, need in _FIT_WINDOWS:
        xs: List[float] = []
        ys: List[float] = []
        for t, tm, tc in zip(times, medium, center):
            if t < hold_t or abs(tm - hold_c) > 1e-9 or tc >= hold_c:
                continue
            g = (hold_c - tc) / span
            if g_lo <= g <= g_hi:
                xs.append(t - hold_t)
                ys.append(math.log10(hold_c - tc))
        if len(xs) < need:
            continue

        m = len(xs)
        mean_x = sum(xs) / m
        mean_y = sum(ys) / m
        sxx = sum((x - mean_x) ** 2 for x in xs)
        sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        if sxx <= 0.0 or sxy >= 0.0:
            continue
        slope = sxy / sxx
        intercept = mean_y - slope * mean_x
        syy = sum((y - mean_y) ** 2 for y in ys)
        r2 = (sxy * sxy) / (sxx * syy) if syy > 0.0 else 0.0
        return HeatPenetration(f_h_s=-1.0 / slope,
                               j_h=(10.0 ** intercept) / span,
                               r2=r2,
                               window_s=(xs[0] + hold_t, xs[-1] + hold_t),
                               n_points=m,
                               g_window=(g_hi, g_lo))
    return None


# ──────────────────────────────────────────────────────────────────────
# Presentation — internal engineering output, never briefing copy
# ──────────────────────────────────────────────────────────────────────
INTERNAL_BANNER = (
    "INTERNAL ENGINEERING ANALYSIS — simulated on representative "
    "properties. Not a scheduled process, not validated, not for "
    "publication. A scheduled process is established by a qualified "
    "process authority on measured product data."
)

_RAMP = " .:-=+*#%@"


def _overshoot_note(result: "SimulationResult") -> str:
    """One line on whether the consistent capacity matrix is being run
    below its monotonicity limit, and by how much it actually mattered."""
    if result.options.lumped_capacity:
        return "none possible (lumped capacity)"
    limit = oscillation_free_time_step_s(
        result.container, result.product, result.options)
    hottest = max(max(result.medium_c), result.initial_c)
    measured = max(result.peak_temps_c) - hottest
    if result.options.time_step_s >= limit:
        return f"none (dt {result.options.time_step_s:.1f} s >= limit {limit:.1f} s)"
    return (f"{measured:+.3f} C at the surface — dt {result.options.time_step_s:.1f} s "
            f"is below the {limit:.1f} s consistent-capacity limit; "
            f"cold spot unaffected")


def temperature_map(result: SimulationResult,
                    temps: Optional[Sequence[float]] = None,
                    width_scale: int = 1) -> str:
    """ASCII cross-section of the container, axis in the middle.

    Rows are z (top of the can at the top of the block), columns are r
    mirrored about the axis so the picture reads like the container. The
    ramp runs cold ' ' to hot '@' across the field's own min/max, so the
    cold spot is always visible even on a nearly uniform field.
    """
    field = list(temps if temps is not None else result.final_temps_c)
    n_r = len(result.r_nodes)
    lo, hi = min(field), max(field)
    span = (hi - lo) or 1.0
    lines = []
    for j in range(len(result.z_nodes) - 1, -1, -1):
        row = field[j * n_r:(j + 1) * n_r]
        chars = []
        for value in row:
            level = int(round((value - lo) / span * (len(_RAMP) - 1)))
            chars.append(_RAMP[level] * width_scale)
        lines.append("|" + "".join(reversed(chars))
                     + "".join(chars) + "|"
                     + f"  z={result.z_nodes[j] * 1000:6.1f} mm")
    rule = "+" + "-" * (2 * n_r * width_scale) + "+"
    return "\n".join([rule] + lines + [rule]
                     + [f"  {lo:.1f} C '{_RAMP[0]}' … '{_RAMP[-1]}' {hi:.1f} C"])


def summarize(result: SimulationResult, *, include_map: bool = True) -> str:
    """Plain-text engineering block. Carries INTERNAL_BANNER by design —
    a figure lifted out of a log has to bring its caveat with it."""
    c = result.container
    p = result.product
    hp = result.heat_penetration
    cold_r, cold_z = result.cold_spot_rz
    out = [
        INTERNAL_BANNER,
        "",
        f"Container      {c.name}: {c.diameter_m * 1000:.1f} mm dia "
        f"x {c.height_m * 1000:.1f} mm, {c.volume_m3 * 1e6:.0f} mL",
        f"Product        {p.name}: k={p.conductivity_w_mk:.3f} W/m.K, "
        f"rho={p.density_kg_m3:.0f} kg/m3, cp={p.specific_heat_j_kgk:.0f} "
        f"J/kg.K  (alpha={p.diffusivity_m2_s * 1e7:.3f}e-7 m2/s)",
        f"Initial temp   {result.initial_c:.1f} C",
        f"Medium         {result.schedule.start_c:.1f} C start, "
        f"peak {max(result.medium_c):.2f} C, "
        f"{result.total_time_s / 60.0:.1f} min total",
        "Surface        " + ("isothermal (condensing steam)"
                             if result.options.surface_h_w_m2k is None else
                             f"h = {result.options.surface_h_w_m2k:.0f} "
                             f"W/m2.K"),
        f"Mesh           {result.options.radial_divisions} x "
        f"{result.options.axial_divisions_even} elements, "
        f"dt = {result.options.time_step_s:.1f} s, "
        f"theta = {result.options.theta:.2f}, "
        + ("lumped" if result.options.lumped_capacity else "consistent")
        + " capacity",
        f"Overshoot      {_overshoot_note(result)}",
        "",
        f"Cold spot      r={cold_r * 1000:.1f} mm, z={cold_z * 1000:.1f} mm "
        f"(geometric centre r=0.0, z={c.height_m * 500:.1f} mm)",
        f"Peak centre    {result.peak_center_c:.2f} C",
        f"F0 cold spot   {result.f0_cold_spot_min:.2f} min  "
        f"(ref {F0_REF_C:.2f} C, z={F0_Z_C:.0f} C)",
        f"F0 mass avg    {result.f0_mass_avg_min:.2f} min",
        f"Cook mass avg  {result.cook_mass_avg_min:.1f} min  "
        f"(ref {COOK_REF_C:.0f} C, z={COOK_Z_C:.1f} C)",
        f"Cook surface   {result.cook_surface_min:.1f} min",
    ]
    if hp is not None:
        out.append(
            f"Heat pen.      f_h = {hp.f_h_min:.1f} min, "
            f"j_h = {hp.j_h:.3f}, r2 = {hp.r2:.5f} "
            f"({hp.n_points} points)")
    if include_map:
        out += ["", "Temperature field at the end of the process:", "",
                temperature_map(result)]
    return "\n".join(out)
