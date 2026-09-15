"""The in-container FEM helper is only worth having if it is verified.

A conduction solver that is merely plausible is worse than no solver:
it produces confident numbers about botulinum lethality that nobody can
check. So the solver is pinned to closed-form solutions, not to its own
previous output. Three families of check:

  1. DISCRETE INVARIANTS — the capacity matrix must integrate to
     rho*cp*V, the conduction matrix must annihilate a uniform field,
     and the surface load must integrate to the wetted area. These catch
     an assembly bug (a missing 2*pi*r weight, a dropped Jacobian) at
     machine precision, before any physics is involved.

  2. CLOSED-FORM AGREEMENT — for a step change to an isothermal surface
     the finite cylinder has an exact solution: the Bessel series in r
     times the cosine series in z. The series is re-implemented here,
     independently of the module, and the simulated centre must track it.
     Ball's f_h and j_h for the same geometry must fall out of the
     simulated curve.

  3. PHYSICS THAT MUST HOLD REGARDLESS — the cold spot of a conduction
     pack is its geometric centre, no node may leave the envelope set by
     the medium and the initial temperature, mass-average lethality must
     exceed cold-spot lethality, and a real come-up must deliver less
     lethality than an instantaneous one.

Plus one guard that is not about numerics at all: process_authority.py
forbids publishing F-value targets in briefing text, and this module
produces exactly those. summarize() must keep saying so.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
if str(DOCS) not in sys.path:
    sys.path.insert(0, str(DOCS))

import finite_elements as fe  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Independent analytic reference — deliberately NOT importing anything
# from the module under test beyond the geometry it was handed.
# ──────────────────────────────────────────────────────────────────────
# Roots of J0 and the value of J1 at each, to 10 significant figures.
_J0_ROOTS = (2.404825558, 5.520078110, 8.653727913,
             11.79153444, 14.93091771, 18.07106397, 21.21163663)
_J1_AT_ROOTS = (0.5191474973, -0.3402648065, 0.2714522999, -0.2324598314,
                0.2065464331, -0.1877288025, 0.1732953678)


def analytic_g_center(t_s: float, radius_m: float, height_m: float,
                      alpha: float) -> float:
    """Unaccomplished temperature difference at the centre of a finite
    cylinder whose whole surface is stepped to the medium temperature.

        g = (T_medium - T) / (T_medium - T_initial)

    Product solution: infinite cylinder x infinite slab. Both series are
    summed to enough terms that the truncation is far below the
    tolerances asserted against it.
    """
    cyl = sum(2.0 / (b * j) * math.exp(-b * b * alpha * t_s / radius_m ** 2)
              for b, j in zip(_J0_ROOTS, _J1_AT_ROOTS))
    half = height_m / 2.0
    slab = 0.0
    for n in range(20):
        lam = (2 * n + 1) * math.pi / 2.0
        slab += (4.0 * (-1) ** n / ((2 * n + 1) * math.pi)
                 * math.exp(-lam * lam * alpha * t_s / half ** 2))
    return cyl * slab


CAN = fe.CONTAINERS["307x409"]
PUREE = fe.PRODUCTS["vegetable_puree"]
MEDIUM_C = 121.11
INITIAL_C = 20.0


@pytest.fixture(scope="module")
def step_change():
    """Long process, instantaneous come-up: the verification case."""
    schedule = fe.RetortSchedule(
        start_c=MEDIUM_C,
        steps=(fe.RetortStep(9000.0, MEDIUM_C, ramp=False),))
    return fe.simulate(
        CAN, PUREE, schedule, initial_c=INITIAL_C,
        options=fe.SolveOptions(radial_divisions=12, axial_divisions=20,
                                time_step_s=5.0))


@pytest.fixture(scope="module")
def retort_cycle():
    """An ordinary three-leg cycle: what the helper is actually for."""
    schedule = fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=600.0, hold_c=MEDIUM_C, hold_s=4500.0,
        cool_c=40.0, cool_s=900.0)
    return fe.simulate(
        CAN, PUREE, schedule, initial_c=70.0,
        options=fe.SolveOptions(radial_divisions=10, axial_divisions=16,
                                time_step_s=5.0))


# ──────────────────────────────────────────────────────────────────────
# 1. Discrete invariants
# ──────────────────────────────────────────────────────────────────────
def test_capacity_matrix_integrates_to_the_container_heat_capacity():
    """Sum of the capacity matrix is rho*cp*V, exactly.

    This is the single cheapest check that the axisymmetric weight and
    the Jacobian are both present and neither is applied twice.
    """
    opts = fe.SolveOptions(radial_divisions=7, axial_divisions=9)
    system = fe._assemble(CAN, PUREE, opts)
    total = sum(system.capacity.row_sums())
    exact = (PUREE.density_kg_m3 * PUREE.specific_heat_j_kgk
             * CAN.volume_m3)
    assert total == pytest.approx(exact, rel=1e-12)


def test_conduction_matrix_annihilates_a_uniform_field():
    """A body at one temperature conducts no heat. K.1 = 0."""
    opts = fe.SolveOptions(radial_divisions=7, axial_divisions=9)
    system = fe._assemble(CAN, PUREE, opts)
    flux = system.conduction.matvec([1.0] * system.n_nodes)
    # The row sums ARE the quantity under test, so the scale has to come
    # from somewhere else: the largest diagonal entry of K.
    scale = max(system.conduction.get(i, i) for i in range(system.n_nodes))
    assert max(abs(v) for v in flux) < 1e-12 * scale


def test_surface_load_integrates_to_the_wetted_area():
    """Side wall plus both ends — the whole can is in the retort."""
    opts = fe.SolveOptions(radial_divisions=7, axial_divisions=9)
    system = fe._assemble(CAN, PUREE, opts)
    exact = (2 * math.pi * CAN.radius_m * CAN.height_m
             + 2 * math.pi * CAN.radius_m ** 2)
    assert sum(system.surface_load) == pytest.approx(exact, rel=1e-12)


def test_lumped_capacity_preserves_the_total():
    opts = fe.SolveOptions(radial_divisions=6, axial_divisions=8)
    consistent = fe._assemble(CAN, PUREE, opts).capacity
    lumped = consistent.lump()
    assert sum(lumped.row_sums()) == pytest.approx(
        sum(consistent.row_sums()), rel=1e-12)


def test_axial_divisions_are_forced_even_so_a_node_sits_at_the_centre():
    """The cold spot is the one place nobody should be interpolating."""
    opts = fe.SolveOptions(radial_divisions=5, axial_divisions=9)
    assert opts.axial_divisions_even == 10
    system = fe._assemble(CAN, PUREE, opts)
    assert system.z_nodes[5] == pytest.approx(CAN.height_m / 2.0)


# ──────────────────────────────────────────────────────────────────────
# 2. Closed-form agreement
# ──────────────────────────────────────────────────────────────────────
def test_centre_history_matches_the_analytic_product_solution(step_change):
    """The simulated centre must track the Bessel-times-cosine series.

    Checked from g=0.9 down to g=0.1 — nearly a full log cycle of the
    approach. Below that the series is fine but the FEM is resolving a
    temperature difference of under a degree, and above it the series
    itself needs more terms than are worth carrying here.
    """
    alpha = PUREE.diffusivity_m2_s
    checked = 0
    for t, temp in zip(step_change.times_s, step_change.center_c):
        if t <= 0.0:
            continue
        g_sim = (MEDIUM_C - temp) / (MEDIUM_C - INITIAL_C)
        if not 0.1 <= g_sim <= 0.9:
            continue
        g_exact = analytic_g_center(t, CAN.radius_m, CAN.height_m, alpha)
        assert g_sim == pytest.approx(g_exact, rel=0.02), (
            f"centre temperature diverges from the closed form at "
            f"t={t:.0f} s: simulated g={g_sim:.5f}, exact g={g_exact:.5f}")
        checked += 1
    assert checked > 100, "the comparison window was never entered"


def test_heat_penetration_recovers_ball_f_h_and_j_h(step_change):
    """f_h and j_h are read off the simulated curve, and must agree with
    the closed form for the same geometry: f_h = ln(10)/lambda_1, and
    j_h = 1.6021 * 1.2732 for a finite cylinder heated on every face."""
    hp = step_change.heat_penetration
    assert hp is not None
    assert hp.g_window == (0.30, 0.02), "fell back to a wider fit window"
    assert hp.r2 > 0.9999, f"fit window still holds curvature: r2={hp.r2}"

    exact = fe.analytic_f_h_s(CAN, PUREE)
    assert hp.f_h_s == pytest.approx(exact, rel=0.02), (
        f"f_h {hp.f_h_s / 60:.2f} min vs analytic {exact / 60:.2f} min")
    assert hp.j_h == pytest.approx(fe.BALL_J_FINITE_CYLINDER, rel=0.03)


def test_analytic_f_h_scales_with_the_square_of_the_dimensions():
    """Double every dimension, quadruple f_h — the sanity check that
    catches a diameter entered where a radius was wanted."""
    small = fe.Container("small", 0.06, 0.10)
    big = fe.Container("big", 0.12, 0.20)
    assert (fe.analytic_f_h_s(big, PUREE)
            == pytest.approx(4.0 * fe.analytic_f_h_s(small, PUREE), rel=1e-12))


@pytest.mark.slow
def test_refinement_converges_on_the_closed_form():
    """Halve the mesh and the time step together; the error must fall by
    at least half each time, and the finest run must be within 1%."""
    schedule = fe.RetortSchedule(
        start_c=MEDIUM_C,
        steps=(fe.RetortStep(2400.0, MEDIUM_C, ramp=False),))
    exact = analytic_g_center(2400.0, CAN.radius_m, CAN.height_m,
                              PUREE.diffusivity_m2_s)
    errors = []
    for nr, nz, dt in ((5, 8, 10.0), (10, 16, 5.0), (20, 32, 2.5)):
        result = fe.simulate(
            CAN, PUREE, schedule, initial_c=INITIAL_C,
            options=fe.SolveOptions(radial_divisions=nr, axial_divisions=nz,
                                    time_step_s=dt))
        idx = result.times_s.index(2400.0)
        g = (MEDIUM_C - result.center_c[idx]) / (MEDIUM_C - INITIAL_C)
        errors.append(abs(g - exact) / exact)

    for coarse, fine in zip(errors, errors[1:]):
        assert fine < coarse / 2.0, f"refinement did not converge: {errors}"
    assert errors[-1] < 0.01, f"finest mesh is still {errors[-1]:.2%} out"


# ──────────────────────────────────────────────────────────────────────
# 3. Physics that must hold regardless
# ──────────────────────────────────────────────────────────────────────
def test_cold_spot_is_the_geometric_centre(step_change):
    """For pure conduction in a cylinder heated on every face, the least
    lethality is at r=0, mid-height. If this ever moves, either the mesh
    or the boundary condition is asymmetric."""
    cold_r, cold_z = step_change.cold_spot_rz
    assert cold_r == pytest.approx(0.0, abs=1e-12)
    assert cold_z == pytest.approx(CAN.height_m / 2.0, rel=1e-12)


def test_nothing_undershoots_the_coldest_medium(retort_cycle):
    """Cooling has no artefact to hide behind: no node may fall below the
    coldest medium temperature."""
    coldest = min(min(retort_cycle.medium_c), retort_cycle.initial_c)
    assert min(retort_cycle.trough_temps_c) >= coldest - 1e-6


def test_lumped_capacity_never_overshoots_the_medium(retort_cycle):
    """The monotone configuration must actually be monotone."""
    lumped = fe.simulate(
        CAN, PUREE, retort_cycle.schedule, initial_c=retort_cycle.initial_c,
        options=fe.SolveOptions(radial_divisions=10, axial_divisions=16,
                                time_step_s=5.0, lumped_capacity=True))
    hottest = max(max(lumped.medium_c), lumped.initial_c)
    assert max(lumped.peak_temps_c) <= hottest + 1e-6


def test_consistent_capacity_overshoot_is_bounded_and_not_at_the_cold_spot(
        retort_cycle):
    """The documented artefact, pinned so it cannot grow unnoticed.

    Below oscillation_free_time_step_s() the consistent capacity matrix
    overshoots the medium by a fraction of a degree next to the wall
    while the come-up front passes. What must stay true is that it is
    small, and that it happens at the surface and never at the cold spot
    — the F0 that a scheduled process is judged on must not be borrowing
    heat from a discretisation artefact.
    """
    limit = fe.oscillation_free_time_step_s(CAN, PUREE, retort_cycle.options)
    assert retort_cycle.options.time_step_s < limit, (
        "this test is only meaningful below the monotonicity limit")

    hottest = max(max(retort_cycle.medium_c), retort_cycle.initial_c)
    overshoot = max(retort_cycle.peak_temps_c) - hottest
    assert 0.0 < overshoot < 1.0, f"overshoot grew to {overshoot:.3f} C"

    n_r = len(retort_cycle.r_nodes)
    hottest_node = max(range(len(retort_cycle.peak_temps_c)),
                       key=lambda i: retort_cycle.peak_temps_c[i])
    # Within one element of the wall or an end — i.e. in the front, not
    # in the middle of the pack.
    near_wall = (hottest_node % n_r >= n_r - 2
                 or hottest_node // n_r <= 1
                 or hottest_node // n_r >= len(retort_cycle.z_nodes) - 2)
    assert near_wall, "overshoot appeared away from the heated surface"

    centre = retort_cycle.center_c
    assert max(centre) <= hottest + 1e-9, "the cold spot overshot"


def test_a_time_step_above_the_limit_removes_the_overshoot():
    """The criterion is a claim about the solver, so it gets tested.

    oscillation_free_time_step_s() says dt >= h**2/(6 alpha) integrates
    without overshoot. Run at twice that and there must be none at all.
    """
    schedule = fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=600.0, hold_c=MEDIUM_C, hold_s=3000.0,
        cool_c=40.0, cool_s=600.0)
    opts = fe.SolveOptions(radial_divisions=10, axial_divisions=16,
                           time_step_s=5.0)
    limit = fe.oscillation_free_time_step_s(CAN, PUREE, opts)
    coarse = fe.SolveOptions(radial_divisions=10, axial_divisions=16,
                             time_step_s=2.0 * limit)
    result = fe.simulate(CAN, PUREE, schedule, initial_c=70.0,
                         options=coarse)
    hottest = max(max(result.medium_c), result.initial_c)
    assert max(result.peak_temps_c) <= hottest + 1e-6


def test_mass_average_lethality_exceeds_the_cold_spot(retort_cycle):
    """Everything outside the centre is hotter for longer. The gap is the
    entire argument for agitation and for thinner containers."""
    assert (retort_cycle.f0_mass_avg_min
            > retort_cycle.f0_cold_spot_min * 1.05)
    assert retort_cycle.f0_cold_spot_min == pytest.approx(
        min(retort_cycle.f0_nodes_min), rel=1e-12)


def test_mass_average_lethality_is_not_lethality_of_the_average():
    """L is convex in T, so the mass average of F must exceed the F you
    would get from the mass-average temperature history. Getting this
    backwards is the classic way to over-report a process."""
    schedule = fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=300.0, hold_c=MEDIUM_C, hold_s=3000.0,
        cool_c=40.0, cool_s=600.0)
    result = fe.simulate(
        CAN, PUREE, schedule, initial_c=70.0,
        options=fe.SolveOptions(radial_divisions=8, axial_divisions=12,
                                time_step_s=10.0))
    f_of_average = fe.f_value(result.times_s, result.mass_avg_c)
    assert result.f0_mass_avg_min > f_of_average


def test_a_come_up_is_worth_part_of_its_length_but_not_all_of_it():
    """Ball's 0.42 correction, as a two-sided bracket.

    A come-up is neither free nor fully lethal, so for a fixed TOTAL
    process time, ramping for D then holding must land strictly between
    holding for the whole D + H (a come-up worth all of its length) and
    holding for H alone with the ramp contributing nothing. Comparing at
    fixed hold length instead would only prove that a longer process
    delivers more lethality, which is not the claim.
    """
    come_up_s, hold_s = 1200.0, 3600.0
    mesh = dict(radial_divisions=8, axial_divisions=12, time_step_s=10.0)

    def run(schedule) -> float:
        return fe.simulate(CAN, PUREE, schedule, initial_c=70.0,
                           options=fe.SolveOptions(**mesh)).f0_cold_spot_min

    with_ramp = run(fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=come_up_s, hold_c=MEDIUM_C, hold_s=hold_s,
        cool_c=40.0, cool_s=600.0))
    # Same wall-clock, come-up credited in full.
    full_credit = run(fe.RetortSchedule(start_c=70.0, steps=(
        fe.RetortStep(come_up_s + hold_s, MEDIUM_C, ramp=False),
        fe.RetortStep(600.0, 40.0, ramp=True))))
    # Come-up credited not at all.
    no_credit = run(fe.RetortSchedule(start_c=70.0, steps=(
        fe.RetortStep(hold_s, MEDIUM_C, ramp=False),
        fe.RetortStep(600.0, 40.0, ramp=True))))

    assert no_credit < with_ramp < full_credit
    fraction = ((with_ramp - no_credit) / (full_credit - no_credit))
    assert 0.1 < fraction < 0.9, (
        f"come-up credited at {fraction:.2f} of its length, which is "
        f"outside anything Ball's method would recognise")


def test_a_finite_surface_coefficient_heats_more_slowly_than_steam():
    """h is a resistance in series with the product. Removing the
    isothermal assumption can only slow the centre down."""
    schedule = fe.RetortSchedule(
        start_c=MEDIUM_C,
        steps=(fe.RetortStep(3600.0, MEDIUM_C, ramp=False),))
    mesh = dict(radial_divisions=8, axial_divisions=12, time_step_s=10.0)
    steam = fe.simulate(CAN, PUREE, schedule, initial_c=INITIAL_C,
                        options=fe.SolveOptions(**mesh))
    water = fe.simulate(CAN, PUREE, schedule, initial_c=INITIAL_C,
                        options=fe.SolveOptions(surface_h_w_m2k=150.0,
                                                **mesh))
    assert water.center_c[-1] < steam.center_c[-1]
    assert water.f0_cold_spot_min < steam.f0_cold_spot_min


def test_a_bigger_can_needs_longer(step_change):
    """f_h goes as the square of the dimensions, so the A10 must lag the
    No. 300 on an identical schedule."""
    schedule = fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=600.0, hold_c=MEDIUM_C, hold_s=3600.0,
        cool_c=40.0, cool_s=600.0)
    mesh = dict(radial_divisions=8, axial_divisions=12, time_step_s=10.0)
    small = fe.simulate(CAN, PUREE, schedule, initial_c=70.0,
                        options=fe.SolveOptions(**mesh))
    big = fe.simulate(fe.CONTAINERS["603x700"], PUREE, schedule,
                      initial_c=70.0, options=fe.SolveOptions(**mesh))
    assert big.f0_cold_spot_min < small.f0_cold_spot_min


# ──────────────────────────────────────────────────────────────────────
# Lethality primitives and process schedule
# ──────────────────────────────────────────────────────────────────────
def test_lethal_rate_is_unity_at_the_reference_temperature():
    assert fe.lethal_rate(fe.F0_REF_C) == pytest.approx(1.0)


def test_lethal_rate_falls_one_decade_per_z():
    assert fe.lethal_rate(fe.F0_REF_C - fe.F0_Z_C) == pytest.approx(0.1)
    assert fe.lethal_rate(fe.F0_REF_C + fe.F0_Z_C) == pytest.approx(10.0)


def test_f_value_of_a_held_reference_temperature_is_elapsed_minutes():
    times = [float(s) for s in range(0, 601, 10)]
    assert fe.f_value(times, [fe.F0_REF_C] * len(times)) == pytest.approx(10.0)


def test_f_value_rejects_mismatched_series():
    with pytest.raises(ValueError):
        fe.f_value([0.0, 1.0], [100.0])


def test_schedule_ramps_holds_and_cools():
    schedule = fe.RetortSchedule.standard(
        start_c=70.0, come_up_s=600.0, hold_c=121.11, hold_s=3000.0,
        cool_c=40.0, cool_s=600.0)
    assert schedule.temperature_at(0.0) == pytest.approx(70.0)
    assert schedule.temperature_at(300.0) == pytest.approx(95.555)
    assert schedule.temperature_at(600.0) == pytest.approx(121.11)
    assert schedule.temperature_at(2000.0) == pytest.approx(121.11)
    assert schedule.temperature_at(3600.0) == pytest.approx(121.11)
    assert schedule.temperature_at(3900.0) == pytest.approx(80.555)
    assert schedule.temperature_at(4200.0) == pytest.approx(40.0)
    assert schedule.temperature_at(99999.0) == pytest.approx(40.0)
    assert schedule.total_s == pytest.approx(4200.0)


def test_can_code_is_inches_and_sixteenths():
    can = fe.Container.from_can_code("307x409")
    assert can.diameter_m == pytest.approx(0.0873125)   # 3 + 7/16 in
    assert can.height_m == pytest.approx(0.1158875)     # 4 + 9/16 in
    assert fe.Container.from_can_code("603x700").diameter_m == pytest.approx(
        (6 + 3 / 16) * 0.0254)


@pytest.mark.parametrize("bad", ["307", "3x4", "307x40", "abcx409", ""])
def test_can_code_rejects_anything_else(bad):
    with pytest.raises(ValueError):
        fe.Container.from_can_code(bad)


@pytest.mark.parametrize("kwargs", [
    {"radial_divisions": 1}, {"axial_divisions": 0},
    {"time_step_s": 0.0}, {"theta": 1.5},
])
def test_solve_options_reject_impossible_settings(kwargs):
    with pytest.raises(ValueError):
        fe.SolveOptions(**kwargs)


def test_container_and_product_derived_quantities():
    can = fe.Container("test", 0.10, 0.20)
    assert can.radius_m == pytest.approx(0.05)
    assert can.volume_m3 == pytest.approx(math.pi * 0.05 ** 2 * 0.20)
    assert PUREE.diffusivity_m2_s == pytest.approx(
        PUREE.conductivity_w_mk
        / (PUREE.density_kg_m3 * PUREE.specific_heat_j_kgk))


# ──────────────────────────────────────────────────────────────────────
# The publication boundary — process_authority.py rule 4
# ──────────────────────────────────────────────────────────────────────
def test_summary_carries_the_internal_banner(retort_cycle):
    """This module computes exactly the F-values that the Process
    Authority note is forbidden to publish. A figure lifted out of a log
    has to arrive with its caveat attached, so the banner is part of the
    output and not part of the documentation."""
    text = fe.summarize(retort_cycle)
    assert fe.INTERNAL_BANNER in text
    for phrase in ("Not a scheduled process", "not validated",
                   "not for publication", "qualified process authority"):
        assert phrase in text


def test_summary_reports_the_numbers_it_computed(retort_cycle):
    text = fe.summarize(retort_cycle)
    assert f"{retort_cycle.f0_cold_spot_min:.2f}" in text
    assert f"{retort_cycle.peak_center_c:.2f}" in text
    assert "307x409" in text


def test_temperature_map_is_symmetric_and_shaped(retort_cycle):
    """The map mirrors the half-cross-section, so every row is twice the
    node count wide plus the two walls."""
    lines = fe.temperature_map(retort_cycle).splitlines()
    n_r = len(retort_cycle.r_nodes)
    n_z = len(retort_cycle.z_nodes)
    body = [ln for ln in lines if ln.startswith("|")]
    assert len(body) == n_z
    field = body[0].split("|")[1]
    assert len(field) == 2 * n_r
    assert field == field[::-1], "cross-section is not symmetric about r=0"
