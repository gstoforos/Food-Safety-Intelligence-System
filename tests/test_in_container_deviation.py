"""Deviation evaluation is the one place this solver touches a decision
about a real lot, so it is the one place a wrong number releases product.

The module follows the NumeriCAL / FMC-patent shape: a numerical model of
the cold spot, calibrated to plant heat-penetration data, run on the
measured retort trace, with the process extended until delivered plus
predicted lethality meets the target. These tests pin each of those
pieces to something that can be checked independently:

  * the closed-form diffusivity inverse round-trips the closed form;
  * calibration recovers a known diffusivity from a curve the solver
    itself produced under a real come-up — the model must at least be
    able to identify itself;
  * a warm-started run is the continuous run to round-off, because the
    extension search relies on it;
  * an unfired deviation costs nothing, a drop costs lethality, the
    proportional correction restores the target and never over-shoots
    by more than a step, the commercial rule costs more than the
    proportional one, and a lot that has already cooled is reported
    UNDER-PROCESSED rather than corrected;
  * the summary carries the internal banner and names 113.89 where it
    must.

Every run here is on a coarse mesh and a long step: these are tests of
logic and monotonicity, not of accuracy, and the accuracy tests live in
test_finite_elements.py.
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
import in_container_deviation as dv  # noqa: E402

CAN = fe.CONTAINERS["307x409"]
PUREE = fe.PRODUCTS["vegetable_puree"]
COARSE = fe.SolveOptions(radial_divisions=6, axial_divisions=10,
                         time_step_s=15.0)
SCHED = fe.RetortSchedule.standard(start_c=70.0, come_up_s=600.0,
                                   hold_c=121.11, hold_s=4800.0,
                                   cool_c=40.0, cool_s=900.0)


# ──────────────────────────────────────────────────────────────────────
# Schedule machinery the module stands on
# ──────────────────────────────────────────────────────────────────────
def test_breakpoints_round_trip_through_from_profile():
    again = fe.RetortSchedule.from_profile(SCHED.breakpoints())
    for t in range(0, int(SCHED.total_s) + 1, 37):
        assert again.temperature_at(t) == pytest.approx(SCHED.temperature_at(t))
    assert again.hold_end_s == SCHED.hold_end_s == 5400.0


def test_a_step_change_survives_the_profile_representation():
    """Two points at one instant are a jump, not a zero-length ramp."""
    sch = fe.RetortSchedule.from_profile([(0, 70), (100, 70), (100, 120),
                                          (400, 120)])
    assert sch.temperature_at(99.9) == pytest.approx(70.0)
    assert sch.temperature_at(100.0) == pytest.approx(120.0)
    assert sch.temperature_at(250.0) == pytest.approx(120.0)


def test_from_profile_rejects_a_trace_that_runs_backwards():
    with pytest.raises(ValueError):
        fe.RetortSchedule.from_profile([(0, 70), (100, 80), (50, 90)])


def test_a_spliced_drop_touches_nothing_outside_its_window():
    dev = SCHED.with_temperature_drop(start_s=1800.0, duration_s=900.0,
                                      low_c=110.0, ramp_down_s=60.0,
                                      ramp_up_s=120.0)
    for t in (0, 300, 600, 1799, 2701, 4000, 5400, 5700, 6300):
        assert dev.temperature_at(t) == pytest.approx(SCHED.temperature_at(t))
    assert dev.temperature_at(1860.0) == pytest.approx(110.0)
    assert dev.temperature_at(2500.0) == pytest.approx(110.0)
    assert 110.0 < dev.temperature_at(2650.0) < 121.11


def test_a_spliced_drop_rejects_ramps_longer_than_itself():
    with pytest.raises(ValueError):
        SCHED.with_temperature_drop(start_s=1800.0, duration_s=100.0,
                                    low_c=110.0, ramp_down_s=60.0,
                                    ramp_up_s=60.0)


def test_warm_start_is_the_continuous_run():
    """The extension search solves the head once and the tails many
    times. That is only legitimate if the seam is invisible."""
    whole = fe.simulate(CAN, PUREE, SCHED, initial_c=70.0, options=COARSE)
    cut = 2700.0
    head = fe.RetortSchedule.from_profile(
        [(t, v) for t, v in SCHED.breakpoints() if t < cut]
        + [(cut, SCHED.temperature_at(cut))])
    first = fe.simulate(CAN, PUREE, head, initial_c=70.0, options=COARSE)
    second = fe.simulate(CAN, PUREE, SCHED.tail_from(cut), initial_c=70.0,
                         options=COARSE, warm_start=first)
    assert second.total_time_s == pytest.approx(whole.total_time_s)
    assert second.f0_cold_spot_min == pytest.approx(whole.f0_cold_spot_min,
                                                    rel=1e-12)
    assert second.cook_mass_avg_min == pytest.approx(whole.cook_mass_avg_min,
                                                     rel=1e-12)
    for a, b in zip(second.final_temps_c, whole.final_temps_c):
        assert a == pytest.approx(b, rel=1e-12)


def test_warm_start_refuses_a_different_mesh():
    first = fe.simulate(CAN, PUREE, SCHED, initial_c=70.0, options=COARSE)
    other = fe.SolveOptions(radial_divisions=5, axial_divisions=8,
                            time_step_s=15.0)
    with pytest.raises(ValueError):
        fe.simulate(CAN, PUREE, SCHED.tail_from(5400.0), initial_c=70.0,
                    options=other, warm_start=first)


# ──────────────────────────────────────────────────────────────────────
# Calibration to heat-penetration data
# ──────────────────────────────────────────────────────────────────────
def test_the_diffusivity_inverse_round_trips_the_closed_form():
    alpha = PUREE.diffusivity_m2_s
    fh = fe.analytic_f_h_s(CAN, PUREE)
    assert dv.apparent_diffusivity_from_fh(CAN, fh) == pytest.approx(
        alpha, rel=1e-12)


def test_calibration_recovers_a_known_diffusivity():
    """Generate a heating curve with a known alpha under a real come-up,
    hand its f_h to the calibrator with a wrong starting product, and
    the calibrator must find its way back."""
    truth = fe.simulate(CAN, PUREE, SCHED, initial_c=70.0, options=COARSE)
    fh = truth.heat_penetration.f_h_s
    wrong = fe.Product("guess", 0.30, PUREE.density_kg_m3,
                       PUREE.specific_heat_j_kgk)
    cal = dv.calibrate_to_heat_penetration(CAN, wrong, fh, SCHED,
                                           initial_c=70.0, options=COARSE)
    assert cal.product.diffusivity_m2_s == pytest.approx(
        PUREE.diffusivity_m2_s, rel=0.01)
    assert abs(cal.fh_error) < 0.002
    assert cal.product.density_kg_m3 == PUREE.density_kg_m3
    assert cal.iterations <= 6


def test_calibration_needs_a_hold_to_read():
    short = fe.RetortSchedule.standard(start_c=70.0, come_up_s=600.0,
                                       hold_c=121.11, hold_s=120.0)
    with pytest.raises(ValueError):
        dv.calibrate_to_heat_penetration(CAN, PUREE, 4000.0, short,
                                         initial_c=70.0, options=COARSE)


# ──────────────────────────────────────────────────────────────────────
# The evaluation
# ──────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def drop():
    actual = SCHED.with_temperature_drop(start_s=1800.0, duration_s=900.0,
                                         low_c=112.0, ramp_down_s=60.0,
                                         ramp_up_s=120.0)
    return dv.evaluate_deviation(CAN, PUREE, SCHED, actual, initial_c=70.0,
                                 options=COARSE, tol_s=30.0)


def test_no_deviation_needs_no_correction():
    ev = dv.evaluate_deviation(CAN, PUREE, SCHED, SCHED, initial_c=70.0,
                               options=COARSE, tol_s=30.0)
    assert ev.verdict == dv.VERDICT_ADEQUATE
    assert ev.deviation_start_s == ev.deviation_end_s == 0.0
    prop = ev.correction("proportional")
    assert prop is not None and prop.extra_hold_s == 0.0
    assert ev.correction("commercial") is None
    assert ev.correction("optimum") is None


def test_a_drop_costs_lethality_and_is_located(drop):
    assert drop.deviation_low_c == pytest.approx(112.0)
    assert 1800.0 <= drop.deviation_start_s <= 1860.0
    assert 2580.0 <= drop.deviation_end_s <= 2700.0
    assert drop.target_f_min == pytest.approx(drop.f_scheduled_min)
    assert drop.f_delivered_at_clearance_min < drop.f_scheduled_min
    assert drop.verdict == dv.VERDICT_CORRECTABLE


def test_the_proportional_correction_restores_the_target_without_excess(drop):
    prop = drop.correction("proportional")
    assert prop.feasible and prop.extra_hold_s > 0.0
    assert prop.f_cold_spot_min >= drop.target_f_min
    # one time step of extension is worth a bounded amount of F; the
    # search must not stop a whole minute above the target
    assert prop.f_cold_spot_min < drop.target_f_min * 1.05
    assert prop.extra_hold_s % COARSE.time_step_s == pytest.approx(0.0)


def test_the_commercial_rule_over_processes(drop):
    """Simpson et al. 2006: charging the whole process for a fault that
    lasted minutes is safe, and expensive. The model shows the bill."""
    com, prop = drop.correction("commercial"), drop.correction("proportional")
    assert com.feasible
    assert com.extra_hold_s > 2.0 * prop.extra_hold_s
    assert com.f_cold_spot_min > 2.0 * drop.target_f_min
    assert com.cook_mass_avg_min > prop.cook_mass_avg_min


def test_the_optimum_correction_finishes_on_the_clock(drop):
    opt = drop.correction("optimum")
    assert opt.feasible
    assert opt.extra_hold_s == 0.0
    assert opt.hold_c > 121.11
    assert opt.f_cold_spot_min >= drop.target_f_min
    assert opt.total_time_s == pytest.approx(drop.scheduled.total_time_s)


def test_a_deeper_drop_needs_a_longer_extension():
    shallow = SCHED.with_temperature_drop(start_s=1800.0, duration_s=600.0,
                                          low_c=116.0)
    deep = SCHED.with_temperature_drop(start_s=1800.0, duration_s=600.0,
                                       low_c=108.0)
    a = dv.evaluate_deviation(CAN, PUREE, SCHED, shallow, initial_c=70.0,
                              options=COARSE, tol_s=30.0)
    b = dv.evaluate_deviation(CAN, PUREE, SCHED, deep, initial_c=70.0,
                              options=COARSE, tol_s=30.0)
    assert b.correction("proportional").extra_hold_s \
        > a.correction("proportional").extra_hold_s


def test_a_lot_that_has_cooled_is_under_processed_not_corrected():
    actual = SCHED.with_temperature_drop(start_s=1800.0, duration_s=900.0,
                                         low_c=112.0)
    ev = dv.evaluate_deviation(CAN, PUREE, SCHED, actual, initial_c=70.0,
                               options=COARSE, tol_s=30.0,
                               process_complete=True)
    assert ev.verdict == dv.VERDICT_UNDER_PROCESSED
    assert ev.as_run is not None
    assert ev.shortfall_min > 0.0
    assert "113.89" in ev.reason


def test_a_target_below_what_was_delivered_is_adequate():
    actual = SCHED.with_temperature_drop(start_s=1800.0, duration_s=900.0,
                                         low_c=112.0)
    ev = dv.evaluate_deviation(CAN, PUREE, SCHED, actual, initial_c=70.0,
                               options=COARSE, tol_s=30.0, target_f_min=3.0)
    assert ev.verdict == dv.VERDICT_ADEQUATE


def test_an_unreachable_target_is_reported_not_faked():
    actual = SCHED.with_temperature_drop(start_s=1800.0, duration_s=900.0,
                                         low_c=112.0)
    ev = dv.evaluate_deviation(CAN, PUREE, SCHED, actual, initial_c=70.0,
                               options=COARSE, tol_s=30.0,
                               target_f_min=500.0, max_extension_s=600.0)
    assert ev.verdict == dv.VERDICT_UNDER_PROCESSED
    prop = ev.correction("proportional")
    assert prop is not None and not prop.feasible
    assert math.isnan(prop.extra_hold_s)


def test_hold_time_for_target_is_monotone_in_the_target():
    short = dv.hold_time_for_target(
        CAN, PUREE, target_f_min=3.0, initial_c=70.0, medium_start_c=70.0,
        come_up_s=600.0, hold_c=121.11, cool_c=40.0, cool_s=900.0,
        options=COARSE, tol_s=30.0)
    long = dv.hold_time_for_target(
        CAN, PUREE, target_f_min=9.0, initial_c=70.0, medium_start_c=70.0,
        come_up_s=600.0, hold_c=121.11, cool_c=40.0, cool_s=900.0,
        options=COARSE, tol_s=30.0)
    assert long > short > 0.0


# ──────────────────────────────────────────────────────────────────────
# The publication boundary
# ──────────────────────────────────────────────────────────────────────
def test_summary_carries_the_banner_and_leaves_the_choice_to_a_person(drop):
    text = dv.summarize(drop)
    assert fe.INTERNAL_BANNER in text
    assert "a process authority chooses; this module does not" in text
    for name in ("proportional", "commercial", "optimum"):
        assert name in text
