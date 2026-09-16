"""The tool page carries its own copy of the solver, so it gets its own test.

docs/tools/in-container-process.html computes in the browser. That means a
second implementation of the conduction solver exists — in JavaScript, in a
file nobody runs pytest against — and a second implementation is a second
chance to be wrong. The Python module is the verified one: it is checked
against the closed-form finite-cylinder solution, against Ball's f_h and
j_h, and against the invariants of its own assembly. The page is only worth
as much as its agreement with that.

So this test slices the solver out of the page between its @solver-start and
@solver-end markers, runs it under node over a spread of cases — step change,
ramped cycle, convective surface, lumped capacity, Crank-Nicolson, a
pasteurisation basis, a big can — and requires the two to agree to 1e-9 on
every reported quantity. Floating-point round-off is the only difference
allowed.

Skipped, not failed, where node is absent: the page is a deliverable rather
than part of the pipeline, and a Python-only CI box should not go red over a
runtime it was never given.
"""
from __future__ import annotations

import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
PAGE = DOCS / "tools" / "in-container-process.html"
if str(DOCS) not in sys.path:
    sys.path.insert(0, str(DOCS))

import finite_elements as fe  # noqa: E402
import in_container_deviation as dv  # noqa: E402

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")

START, END = "/* @solver-start", "/* @solver-end */"

# name, (diameter, height), (k, rho, cp), initial, schedule, options
CASES = [
    ("default retort cycle", (0.0873125, 0.1158875), (0.55, 1080, 3700), 70.0,
     (70.0, [(600, 121.11, True), (4500, 121.11, False), (900, 40, True)]),
     dict(nr=10, nz=16, dt=5.0)),
    ("step change, long", (0.0873125, 0.1158875), (0.55, 1080, 3700), 20.0,
     (121.11, [(9000, 121.11, False)]),
     dict(nr=12, nz=20, dt=5.0)),
    ("convective surface", (0.0873125, 0.1158875), (0.55, 1080, 3700), 20.0,
     (20.0, [(3600, 121.11, False)]),
     dict(nr=8, nz=12, dt=10.0, h=150.0)),
    ("lumped capacity", (0.0873125, 0.1158875), (0.55, 1080, 3700), 70.0,
     (70.0, [(600, 121.11, True), (3000, 121.11, False), (600, 40, True)]),
     dict(nr=10, nz=16, dt=5.0, lumped=True)),
    ("crank-nicolson", (0.0873125, 0.1158875), (0.55, 1080, 3700), 70.0,
     (70.0, [(600, 121.11, True), (3000, 121.11, False), (600, 40, True)]),
     dict(nr=10, nz=16, dt=5.0, theta=0.5)),
    ("pasteurisation basis", (0.0873125, 0.1158875), (0.55, 1080, 3700), 10.0,
     (10.0, [(600, 90, True), (3600, 90, False), (1200, 4, True)]),
     dict(nr=10, nz=16, dt=5.0, f0Ref=70.0, f0Z=7.5)),
    ("A10 big can", (0.1571625, 0.1778), (0.45, 1050, 3500), 60.0,
     (60.0, [(900, 118, True), (7200, 118, False), (1800, 40, True)]),
     dict(nr=9, nz=14, dt=10.0)),
]


def _case_json():
    out = []
    for name, can, prod, init, (start, steps), opt in CASES:
        out.append({
            "name": name, "can": list(can), "prod": list(prod), "init": init,
            "sch": {"start": start,
                    "steps": [[d, t, 1 if r else 0] for d, t, r in steps]},
            "opt": {"nr": opt.get("nr", 10), "nz": opt.get("nz", 16),
                    "dt": opt.get("dt", 5.0), "theta": opt.get("theta", 1.0),
                    "h": opt.get("h"), "lumped": 1 if opt.get("lumped") else 0,
                    "f0Ref": opt.get("f0Ref", 121.11), "f0Z": opt.get("f0Z", 10.0),
                    "cookRef": opt.get("cookRef", 100.0),
                    "cookZ": opt.get("cookZ", 33.1)},
        })
    return out


def test_the_page_carries_the_solver_between_its_markers():
    """If this fails the slice below is silently testing nothing."""
    src = PAGE.read_text(encoding="utf-8")
    assert START in src and END in src
    assert src.index(START) < src.index(END)
    block = src[src.index(START):src.index(END)]
    assert "function simulate(cfg)" in block
    assert "Band.prototype.factorise" in block


def _run_node(cases):
    src = PAGE.read_text(encoding="utf-8")
    block = src[src.index(START):src.index(END) + len(END)]
    harness = block + """
const cases = %s;
const out = [];
for (const c of cases){
  const can = { diameter_m:c.can[0], height_m:c.can[1] };
  const prod = { k:c.prod[0], rho:c.prod[1], cp:c.prod[2] };
  const sch = { start_c:c.sch.start,
                steps:c.sch.steps.map(s => ({ duration_s:s[0], target_c:s[1], ramp:!!s[2] })) };
  const o = c.opt;
  const r = FEM.simulate({ container:can, product:prod, schedule:sch,
    initial_c:c.init,
    options:{ nr:o.nr, nz:o.nz, dt:o.dt, theta:o.theta, h:o.h, lumped:!!o.lumped,
              f0Ref:o.f0Ref, f0Z:o.f0Z, cookRef:o.cookRef, cookZ:o.cookZ } });
  const hp = r.heatPenetration;
  let mass = 0; for (const m of r.massWeights) mass += m;
  out.push({ name:c.name, f0Cold:r.f0ColdSpot, f0Avg:r.f0MassAvg,
    cookAvg:r.cookMassAvg, cookSurf:r.cookSurface, peakCentre:r.peakCentre,
    maxPeak:Math.max.apply(null, Array.from(r.peakTemps)),
    minTrough:Math.min.apply(null, Array.from(r.troughTemps)),
    coldR:r.coldSpot.r, coldZ:r.coldSpot.z, mass:mass,
    fh:hp?hp.fhS:null, jh:hp?hp.jH:null, r2:hp?hp.r2:null,
    npts:hp?hp.nPoints:null, nsteps:r.times.length });
}
console.log(JSON.stringify(out));
""" % json.dumps(cases)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                     encoding="utf-8") as fh:
        fh.write(harness)
        path = fh.name
    try:
        proc = subprocess.run([NODE, path], capture_output=True, text=True,
                              timeout=300)
    finally:
        Path(path).unlink(missing_ok=True)
    assert proc.returncode == 0, f"node failed:\n{proc.stderr[-2000:]}"
    return json.loads(proc.stdout)


def _run_python(case):
    can = fe.Container(case["name"], case["can"][0], case["can"][1])
    prod = fe.Product("p", *case["prod"])
    sched = fe.RetortSchedule(
        start_c=case["sch"]["start"],
        steps=tuple(fe.RetortStep(d, t, ramp=bool(r))
                    for d, t, r in case["sch"]["steps"]))
    o = case["opt"]
    opts = fe.SolveOptions(
        radial_divisions=o["nr"], axial_divisions=o["nz"],
        time_step_s=o["dt"], theta=o["theta"], surface_h_w_m2k=o["h"],
        lumped_capacity=bool(o["lumped"]), f0_ref_c=o["f0Ref"],
        f0_z_c=o["f0Z"], cook_ref_c=o["cookRef"], cook_z_c=o["cookZ"])
    r = fe.simulate(can, prod, sched, initial_c=case["init"], options=opts)
    hp = r.heat_penetration
    return {
        "f0Cold": r.f0_cold_spot_min, "f0Avg": r.f0_mass_avg_min,
        "cookAvg": r.cook_mass_avg_min, "cookSurf": r.cook_surface_min,
        "peakCentre": r.peak_center_c,
        "maxPeak": max(r.peak_temps_c), "minTrough": min(r.trough_temps_c),
        "coldR": r.cold_spot_rz[0], "coldZ": r.cold_spot_rz[1],
        "mass": sum(r.mass_weights),
        "fh": hp.f_h_s if hp else None, "jh": hp.j_h if hp else None,
        "r2": hp.r2 if hp else None, "npts": hp.n_points if hp else None,
        "nsteps": len(r.times_s),
    }


@pytest.fixture(scope="module")
def both():
    cases = _case_json()
    return cases, _run_node(cases)


@pytest.mark.slow
def test_the_page_solver_reproduces_the_verified_python(both):
    """Two implementations, one answer, to floating-point round-off."""
    cases, js = both
    assert [c["name"] for c in cases] == [r["name"] for r in js]

    worst, where = 0.0, ""
    for case, got in zip(cases, js):
        want = _run_python(case)
        for key, wv in want.items():
            gv = got[key]
            if wv is None or gv is None:
                assert wv is gv, f"{case['name']}/{key}: one side is None"
                continue
            rel = 0.0 if wv == gv else abs(wv - gv) / max(abs(wv), abs(gv), 1e-12)
            if rel > worst:
                worst, where = rel, f"{case['name']}/{key}"
            assert rel < 1e-9, (
                f"the page and the module disagree on {case['name']}/{key}: "
                f"python {wv!r}, page {gv!r} (relative {rel:.3e})")
    assert worst < 1e-9, where


# ──────────────────────────────────────────────────────────────────────
# The deviation port
# ──────────────────────────────────────────────────────────────────────
# The page evaluates deviations with a port of in_container_deviation.py:
# the same schedule splicing, the same warm-started extension search, the
# same three corrections. A disagreement here is a disagreement about how
# much longer a deviated lot must cook, so the tolerance is round-off.
DEV_OPTS = dict(nr=6, nz=10, dt=15.0)
DEV_SCENARIO = dict(start_c=70.0, come_up_s=600.0, hold_c=121.11,
                    hold_s=4800.0, cool_c=40.0, cool_s=900.0,
                    drop_start_s=1800.0, drop_s=900.0, low_c=112.0,
                    ramp_down_s=60.0, ramp_up_s=120.0, initial_c=70.0,
                    tol_s=30.0)


def _run_node_deviation(complete: bool):
    src = PAGE.read_text(encoding="utf-8")
    block = src[src.index(START):src.index(END) + len(END)]
    harness = block + """
const S = %s, O = %s, complete = %s;
const can = { diameter_m: %r, height_m: %r }, prod = { k: %r, rho: %r, cp: %r };
const sched = FEM.standardSchedule(S.start_c, S.come_up_s, S.hold_c, S.hold_s, S.cool_c, S.cool_s);
const actual = FEM.withTemperatureDrop(sched, { start_s: S.drop_start_s, duration_s: S.drop_s,
  low_c: S.low_c, ramp_down_s: S.ramp_down_s, ramp_up_s: S.ramp_up_s });
const ev = FEM.evaluateDeviation(can, prod, sched, actual,
  { initial_c: S.initial_c, options: O, tol_s: S.tol_s, process_complete: complete });
const out = { target: ev.target_f_min, low: ev.deviation_low_c, start: ev.deviation_start_s,
  end: ev.deviation_end_s, holdEnd: ev.hold_end_s, fSched: ev.f_scheduled_min,
  fClear: ev.f_delivered_at_clearance_min, fAsRun: ev.f_as_run_min, verdict: ev.verdict,
  corrections: ev.corrections.map(c => ({ name: c.name, feasible: c.feasible,
    extra: c.extra_hold_s, holdC: c.hold_c, f: c.f_cold_spot_min,
    cook: c.cook_mass_avg_min, total: c.total_time_s })) };
// the calibration path, on the curve the model itself produced
const truth = FEM.simulate({ container: can, product: prod, schedule: sched, initial_c: S.initial_c, options: O });
const cal = FEM.calibrateToFh(can, { k: 0.30, rho: prod.rho, cp: prod.cp }, truth.heatPenetration.fhS,
                              sched, S.initial_c, O);
out.cal = { alpha: cal.alphaCalibrated, fh: cal.fhSimulated, it: cal.iterations, k: cal.product.k };
console.log(JSON.stringify(out));
""" % (json.dumps(DEV_SCENARIO), json.dumps(DEV_OPTS), "true" if complete else "false",
       CASES[0][1][0], CASES[0][1][1], CASES[0][2][0], CASES[0][2][1], CASES[0][2][2])
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                     encoding="utf-8") as fh:
        fh.write(harness)
        path = fh.name
    try:
        proc = subprocess.run([NODE, path], capture_output=True, text=True,
                              timeout=600)
    finally:
        Path(path).unlink(missing_ok=True)
    assert proc.returncode == 0, f"node failed:\n{proc.stderr[-2000:]}"
    return json.loads(proc.stdout)


def _run_python_deviation(complete: bool):
    S = DEV_SCENARIO
    can = fe.Container("307x409", CASES[0][1][0], CASES[0][1][1])
    prod = fe.Product("p", *CASES[0][2])
    opts = fe.SolveOptions(radial_divisions=DEV_OPTS["nr"],
                           axial_divisions=DEV_OPTS["nz"],
                           time_step_s=DEV_OPTS["dt"])
    sched = fe.RetortSchedule.standard(
        start_c=S["start_c"], come_up_s=S["come_up_s"], hold_c=S["hold_c"],
        hold_s=S["hold_s"], cool_c=S["cool_c"], cool_s=S["cool_s"])
    actual = sched.with_temperature_drop(
        start_s=S["drop_start_s"], duration_s=S["drop_s"], low_c=S["low_c"],
        ramp_down_s=S["ramp_down_s"], ramp_up_s=S["ramp_up_s"])
    ev = dv.evaluate_deviation(can, prod, sched, actual,
                               initial_c=S["initial_c"], options=opts,
                               tol_s=S["tol_s"], process_complete=complete)
    truth = fe.simulate(can, prod, sched, initial_c=S["initial_c"],
                        options=opts)
    cal = dv.calibrate_to_heat_penetration(
        can, fe.Product("g", 0.30, prod.density_kg_m3,
                        prod.specific_heat_j_kgk),
        truth.heat_penetration.f_h_s, sched, initial_c=S["initial_c"],
        options=opts)
    return {
        "target": ev.target_f_min, "low": ev.deviation_low_c,
        "start": ev.deviation_start_s, "end": ev.deviation_end_s,
        "holdEnd": ev.hold_end_s, "fSched": ev.f_scheduled_min,
        "fClear": ev.f_delivered_at_clearance_min,
        "fAsRun": ev.f_as_run_min, "verdict": ev.verdict,
        "corrections": [
            {"name": c.name, "feasible": c.feasible, "extra": c.extra_hold_s,
             "holdC": c.hold_c, "f": c.f_cold_spot_min,
             "cook": c.cook_mass_avg_min, "total": c.total_time_s}
            for c in ev.corrections],
        "cal": {"alpha": cal.alpha_calibrated, "fh": cal.fh_simulated_s,
                "it": cal.iterations, "k": cal.product.conductivity_w_mk},
    }


def _same(a, b, where):
    if isinstance(a, dict):
        assert isinstance(b, dict) and set(a) == set(b), where
        for k in a:
            _same(a[k], b[k], f"{where}.{k}")
    elif isinstance(a, list):
        assert len(a) == len(b), where
        for i, (x, y) in enumerate(zip(a, b)):
            _same(x, y, f"{where}[{i}]")
    elif isinstance(a, bool) or isinstance(a, str) or a is None:
        assert a == b, f"{where}: python {a!r}, page {b!r}"
    else:
        if math.isnan(a):
            assert b is None or math.isnan(b), f"{where}: python nan, page {b!r}"
            return
        rel = 0.0 if a == b else abs(a - b) / max(abs(a), abs(b), 1e-12)
        assert rel < 1e-9, f"{where}: python {a!r}, page {b!r} ({rel:.2e})"


@pytest.mark.slow
@pytest.mark.parametrize("complete", [False, True])
def test_the_page_evaluates_a_deviation_exactly_as_the_module_does(complete):
    """Verdict, location, depth, every correction's extension, lethality,
    cook value and total time, and the calibrated diffusivity — all to
    round-off, in both the correctable and the already-cooled cases."""
    want = _run_python_deviation(complete)
    got = _run_node_deviation(complete)
    _same(want, got, "deviation")
    assert want["verdict"] == (dv.VERDICT_UNDER_PROCESSED if complete
                               else dv.VERDICT_CORRECTABLE)
