"""
in_container_deviation.py
=========================

Process-deviation evaluation for in-container thermal processes, on top
of the finite-element solver in finite_elements.py. A helper in the same
family: pure library, no CLI, no I/O, no network, nothing in the pipeline
depends on it.

WHAT A DEVIATION IS, AND WHO GETS TO DECIDE
-------------------------------------------
21 CFR 113.89: any departure from the scheduled process — the retort
falling below the scheduled temperature, a short come-up, a lost vent, a
timing error — puts the affected lot on hold, and the lot is either fully
reprocessed or evaluated by a competent process authority, with records.
Every other jurisdiction in the PA note converges on the same principle
under its own instrument. This module is the evaluation arithmetic. It
is not the process authority, and it says so on every summary it prints.

HOW THE INDUSTRY DOES IT — the NumeriCAL lineage
------------------------------------------------
The commercial reference for this work is JBT's NumeriCAL (originally
FMC FoodTech), and its method is on the public record in the FMC patents
on on-line deviation handling (Weng, US 6,472,008 for batch retorts;
6,416,711 rotary; 6,440,361 hydrostatic) and in the papers behind them
(Teixeira, Dixon, Zahradnik & Zinsmeister 1969; Datta, Teixeira & Manson
1986; Teixeira & Tucker 1997; Simpson, Almonacid & Teixeira 2006, 2007).
The shape of it:

  1. A NUMERICAL MODEL OF THE COLD SPOT, not a formula. Ball's formula
     method assumes a constant retort temperature, so it cannot represent
     a deviation at all; the General method needs a measured cold-spot
     trace, which a deviated lot usually lacks. A finite-difference (here
     finite-element) solution of Fourier's equation takes the retort
     temperature as an arbitrary function of time and produces the
     cold-spot history under it, cooling included.

  2. THE MODEL IS CALIBRATED TO PLANT HEAT-PENETRATION DATA. Handbook
     properties are not used. The heating factor f_h measured in the
     plant's own heat-penetration study is turned into the apparent
     thermal diffusivity the model needs, so the model reproduces the
     product as it actually heats in that container in that retort.
     NumeriCAL "imports the heating and cooling factors" for this reason.
     calibrate_to_heat_penetration() is that step.

  3. TOTAL LETHALITY = DELIVERED + PREDICTED. From the patent: the
     lethality actually delivered from the start of the process to the
     moment the deviation clears, computed by the model on the measured
     retort trace, plus the lethality predicted over the remainder; the
     end of the process is moved until the sum meets the target.
     evaluate_deviation() does exactly this, with the target defaulting
     to what the SCHEDULED process delivers to the cold spot, cooling
     included — "the original target lethality" of Datta et al.

  4. THREE CORRECTIONS, from safest to most economical:
       commercial   extend as though the WHOLE process had run at the
                    lowest temperature reached in the deviation. The
                    industry's traditional rule (Simpson et al. 2006
                    describe it as what commercial systems did); safe,
                    and it over-processes because it charges the whole
                    process for a fault that lasted minutes.
       proportional extend the hold at the RECOVERED temperature by the
                    time the model says the cold spot needs, accounting
                    for both the depth and the duration of the drop.
                    This is what a simulation gives directly, and it is
                    the one the patent's method produces.
       optimum      instead of extending, raise the retort temperature
                    for the remaining scheduled time (Simpson et al.
                    2007). Same lethality, no schedule slip, more cook.
     All three are reported. Which to use is an engineering and a
     regulatory decision, not a numerical one.

  5. SOME DEVIATIONS CANNOT BE CORRECTED ON-LINE. If the process has
     already cooled, there is nothing left to extend: the lot is short
     by a known amount and 113.89 offers reprocessing or a PA evaluation
     of the product, not a number from this module.

WHAT THIS MODULE IS NOT
-----------------------
It inherits every limit of the solver: conduction only, uniform medium,
constant properties, no headspace or agitation. It evaluates the cold
spot of ONE container under ONE medium trace; a retort deviation is also
a temperature-distribution question, which is a study and not a model.
And the summary carries the INTERNAL banner of finite_elements.py: the
figures are for a process authority to use, not for anyone to publish.

Public API
----------
    apparent_diffusivity_from_fh(container, fh_s)
    calibrate_to_heat_penetration(...)  -> CalibratedProduct
    hold_time_for_target(...)           -> float (seconds)
    evaluate_deviation(...)             -> DeviationEvaluation
    summarize(evaluation)               -> internal-use text block
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Callable, Optional, Tuple

from finite_elements import (
    Container, Product, RetortSchedule, SolveOptions, SimulationResult,
    INTERNAL_BANNER, simulate, _BESSEL_J0_ROOT_1, _LN10,
)

__all__ = [
    "CalibratedProduct", "DeviationEvaluation", "Correction",
    "apparent_diffusivity_from_fh", "calibrate_to_heat_penetration",
    "hold_time_for_target", "evaluate_deviation", "summarize",
]


# ──────────────────────────────────────────────────────────────────────
# 2. Calibration to heat-penetration data
# ──────────────────────────────────────────────────────────────────────
def apparent_diffusivity_from_fh(container: Container, fh_s: float) -> float:
    """The closed-form inverse of analytic_f_h_s: the diffusivity of an
    ideal conduction pack whose asymptotic heating rate is fh_s.

        alpha = ln(10) / ( f_h * ( (2.4048/R)^2 + (pi/H)^2 ) )

    Exact for an isothermal surface and an instantaneous come-up, which
    a plant's heat-penetration run is not — so this is the starting
    point for calibrate_to_heat_penetration(), not its answer.
    """
    if fh_s <= 0.0:
        raise ValueError("f_h must be positive")
    lam = (_BESSEL_J0_ROOT_1 / container.radius_m) ** 2 \
        + (math.pi / container.height_m) ** 2
    return _LN10 / (fh_s * lam)


@dataclass(frozen=True)
class CalibratedProduct:
    """A Product whose diffusivity reproduces a measured f_h, and the
    evidence for it."""
    product: Product
    fh_measured_s: float
    fh_simulated_s: float
    alpha_closed_form: float
    alpha_calibrated: float
    iterations: int

    @property
    def fh_error(self) -> float:
        return (self.fh_simulated_s - self.fh_measured_s) / self.fh_measured_s


def calibrate_to_heat_penetration(container: Container,
                                  product: Product,
                                  fh_measured_s: float,
                                  schedule: RetortSchedule,
                                  *,
                                  initial_c: float,
                                  options: Optional[SolveOptions] = None,
                                  tolerance: float = 0.002,
                                  max_iterations: int = 12
                                  ) -> CalibratedProduct:
    """Adjust the product's diffusivity until the model's f_h, read off
    the simulated curve UNDER THE PLANT'S OWN COME-UP, matches the f_h
    the plant measured.

    The density and specific heat are kept and the conductivity is
    scaled, because f_h depends on the three only through their ratio:
    alpha = k / (rho cp) is the one number a heating curve identifies.
    Secant iteration from the closed-form estimate; f_h is a smooth,
    monotone function of alpha so it converges in a handful of steps.
    `schedule` should be the heat-penetration run's own profile so the
    fit window and the come-up treatment match the measurement.
    """
    if fh_measured_s <= 0.0:
        raise ValueError("f_h must be positive")
    opts = options or SolveOptions()
    rho_cp = product.density_kg_m3 * product.specific_heat_j_kgk

    def with_alpha(alpha: float) -> Product:
        return replace(product, conductivity_w_mk=alpha * rho_cp)

    def fh_of(alpha: float) -> float:
        res = simulate(container, with_alpha(alpha), schedule,
                       initial_c=initial_c, options=opts)
        if res.heat_penetration is None:
            raise ValueError(
                "the schedule never holds long enough for f_h to be read "
                "off the simulated curve — lengthen the hold")
        return res.heat_penetration.f_h_s

    a0 = apparent_diffusivity_from_fh(container, fh_measured_s)
    f0 = fh_of(a0)
    # f_h ~ 1/alpha, so the first correction is the exact one for an
    # ideal pack and nearly so for a real one.
    a1 = a0 * f0 / fh_measured_s
    f1 = fh_of(a1)
    it = 2
    while abs(f1 - fh_measured_s) > tolerance * fh_measured_s \
            and it < max_iterations:
        # secant in log(alpha) against log(f_h), which is close to linear
        la0, la1 = math.log(a0), math.log(a1)
        lf0, lf1 = math.log(f0), math.log(f1)
        if lf1 == lf0:
            break
        la2 = la1 + (math.log(fh_measured_s) - lf1) * (la1 - la0) / (lf1 - lf0)
        a0, f0 = a1, f1
        a1 = math.exp(la2)
        f1 = fh_of(a1)
        it += 1
    return CalibratedProduct(product=with_alpha(a1), fh_measured_s=fh_measured_s,
                             fh_simulated_s=f1, alpha_closed_form=a0
                             if it == 2 else apparent_diffusivity_from_fh(
                                 container, fh_measured_s),
                             alpha_calibrated=a1, iterations=it)


# ──────────────────────────────────────────────────────────────────────
# Search helpers
# ──────────────────────────────────────────────────────────────────────
def _bisect_increasing(f: Callable[[float], float], target: float,
                       lo: float, hi: float, *, tol_x: float,
                       max_iter: int = 40) -> Tuple[float, float]:
    """Smallest x in [lo, hi] with f(x) >= target, for f non-decreasing.
    Returns (x, f(x)). Raises if even f(hi) falls short."""
    f_hi = f(hi)
    if f_hi < target:
        raise ValueError(
            f"target {target:.3f} not reachable within the search limit "
            f"(reaches {f_hi:.3f})")
    f_lo = f(lo)
    if f_lo >= target:
        return lo, f_lo
    for _ in range(max_iter):
        if hi - lo <= tol_x:
            break
        mid = 0.5 * (lo + hi)
        f_mid = f(mid)
        if f_mid >= target:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return hi, f_hi


def hold_time_for_target(container: Container, product: Product, *,
                         target_f_min: float, initial_c: float,
                         medium_start_c: float, come_up_s: float,
                         hold_c: float, cool_c: float, cool_s: float,
                         options: Optional[SolveOptions] = None,
                         max_hold_s: float = 6.0 * 3600.0,
                         tol_s: float = 15.0) -> float:
    """The hold time at hold_c that delivers target_f_min to the cold
    spot, cooling included. Process design, and the engine behind the
    commercial correction."""
    opts = options or SolveOptions()

    def f_of(hold_s: float) -> float:
        sch = RetortSchedule.standard(start_c=medium_start_c,
                                      come_up_s=come_up_s, hold_c=hold_c,
                                      hold_s=hold_s, cool_c=cool_c,
                                      cool_s=cool_s)
        return simulate(container, product, sch, initial_c=initial_c,
                        options=opts).f0_cold_spot_min

    hold, _ = _bisect_increasing(f_of, target_f_min, opts.time_step_s,
                                 max_hold_s, tol_x=tol_s)
    return hold


# ──────────────────────────────────────────────────────────────────────
# 3–5. The evaluation
# ──────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Correction:
    """One way of making the lot whole, and what it costs."""
    name: str
    description: str
    extra_hold_s: float            # added at the recovered temperature
    hold_c: float                  # temperature of the added / remaining hold
    f_cold_spot_min: float         # what the corrected process delivers
    cook_mass_avg_min: float       # what it costs in quality
    total_time_s: float
    feasible: bool
    note: str = ""


@dataclass(frozen=True)
class DeviationEvaluation:
    scheduled: SimulationResult
    as_run: Optional[SimulationResult]      # the actual trace, if it has cooled
    to_clearance: SimulationResult          # actual trace up to the hold end
    target_f_min: float
    deviation_low_c: float
    deviation_start_s: float
    deviation_end_s: float
    hold_end_s: float
    corrections: Tuple[Correction, ...]
    verdict: str
    reason: str

    @property
    def f_scheduled_min(self) -> float:
        return self.scheduled.f0_cold_spot_min

    @property
    def f_delivered_at_clearance_min(self) -> float:
        return self.to_clearance.f0_cold_spot_min

    @property
    def f_as_run_min(self) -> Optional[float]:
        return self.as_run.f0_cold_spot_min if self.as_run else None

    @property
    def shortfall_min(self) -> Optional[float]:
        if self.as_run is None:
            return None
        return max(0.0, self.target_f_min - self.as_run.f0_cold_spot_min)

    def correction(self, name: str) -> Optional[Correction]:
        for c in self.corrections:
            if c.name == name:
                return c
        return None


VERDICT_ADEQUATE = "ADEQUATE"
VERDICT_CORRECTABLE = "CORRECTABLE"
VERDICT_UNDER_PROCESSED = "UNDER-PROCESSED"


def evaluate_deviation(container: Container,
                       product: Product,
                       scheduled: RetortSchedule,
                       actual: RetortSchedule,
                       *,
                       initial_c: float,
                       options: Optional[SolveOptions] = None,
                       target_f_min: Optional[float] = None,
                       process_complete: bool = False,
                       max_extension_s: float = 4.0 * 3600.0,
                       max_raise_c: float = 8.0,
                       tol_s: float = 10.0) -> DeviationEvaluation:
    """Evaluate a medium-temperature deviation and the ways to correct it.

    scheduled   the filed process (its cooling leg is reused as the tail
                of every correction)
    actual      the medium as it really ran — a logged trace via
                RetortSchedule.from_profile(), or the scheduled profile
                with .with_temperature_drop() spliced in
    target_f_min
                the lethality the lot must reach at the cold spot;
                defaults to what the scheduled process itself delivers,
                cooling included
    process_complete
                True when `actual` runs through cooling and the lot is
                already out of the retort: there is nothing left to
                extend, and the verdict says so

    Returns every number a process authority would ask for, and three
    candidate corrections (commercial, proportional, optimum). It does
    not choose between them.
    """
    opts = options or SolveOptions()
    hold_end = scheduled.hold_end_s
    if hold_end is None:
        raise ValueError("the scheduled process has no constant-temperature "
                         "hold to extend")
    hold_c = scheduled.temperature_at(hold_end)
    dt = opts.time_step_s
    # the warm start needs the clearance instant on the time grid
    hold_end = round(hold_end / dt) * dt

    sched_res = simulate(container, product, scheduled, initial_c=initial_c,
                         options=opts)
    target = target_f_min if target_f_min is not None \
        else sched_res.f0_cold_spot_min

    # -- where and how deep the deviation was ------------------------------
    times = [t for t, _ in actual.breakpoints()]
    grid = sorted(set([0.0, hold_end] + times
                      + [i * dt for i in range(int(hold_end / dt) + 1)]))
    dev_pts = [(t, actual.temperature_at(t)) for t in grid if t <= hold_end]
    deficits = [(t, v) for t, v in dev_pts
                if v < scheduled.temperature_at(t) - 0.05]
    if deficits:
        low_c = min(v for _, v in deficits)
        dev_start = min(t for t, _ in deficits)
        dev_end = max(t for t, _ in deficits)
    else:
        low_c, dev_start, dev_end = hold_c, 0.0, 0.0

    # -- delivered up to clearance, on the actual trace --------------------
    head = actual.from_profile(
        [(t, v) for t, v in actual.breakpoints() if t < hold_end]
        + [(hold_end, actual.temperature_at(hold_end))])
    to_clear = simulate(container, product, head, initial_c=initial_c,
                        options=opts)

    tail = scheduled.tail_from(hold_end)          # the scheduled cooling
    cooled = simulate(container, product, tail, initial_c=initial_c,
                      options=opts, warm_start=to_clear)
    as_run = cooled if process_complete else None

    corrections = []

    # proportional — extend at the recovered temperature -------------------
    def f_extended(extra_s: float) -> SimulationResult:
        ext = RetortSchedule.from_profile(
            [(0.0, actual.temperature_at(hold_end)), (0.0, hold_c),
             (extra_s, hold_c)])
        if extra_s <= 0.0:
            return cooled
        mid = simulate(container, product, ext, initial_c=initial_c,
                       options=opts, warm_start=to_clear)
        return simulate(container, product, tail, initial_c=initial_c,
                        options=opts, warm_start=mid)

    try:
        extra, _ = _bisect_increasing(
            lambda x: f_extended(x).f0_cold_spot_min, target, 0.0,
            max_extension_s, tol_x=tol_s)
        extra = math.ceil(extra / dt) * dt if extra > 0.0 else 0.0
        res = f_extended(extra)
        corrections.append(Correction(
            name="proportional",
            description="extend the hold at the recovered temperature by "
                        "the time the model says the cold spot needs",
            extra_hold_s=extra, hold_c=hold_c,
            f_cold_spot_min=res.f0_cold_spot_min,
            cook_mass_avg_min=res.cook_mass_avg_min,
            total_time_s=res.total_time_s, feasible=True))
    except ValueError as e:
        corrections.append(Correction(
            name="proportional", description="extend at the recovered "
            "temperature", extra_hold_s=float("nan"), hold_c=hold_c,
            f_cold_spot_min=float("nan"), cook_mass_avg_min=float("nan"),
            total_time_s=float("nan"), feasible=False, note=str(e)))

    # commercial — as if the whole process had run at the low temperature --
    sched_pts = scheduled.breakpoints()
    come_up_s = next((t for t, v in sched_pts if abs(v - hold_c) < 1e-9), 0.0)
    sched_hold_s = hold_end - come_up_s
    cool_s = scheduled.total_s - hold_end
    cool_c = scheduled.temperature_at(scheduled.total_s)
    if low_c < hold_c - 0.05:
        try:
            hold_low = hold_time_for_target(
                container, product, target_f_min=target, initial_c=initial_c,
                medium_start_c=scheduled.start_c, come_up_s=come_up_s,
                hold_c=low_c, cool_c=cool_c, cool_s=cool_s, options=opts,
                max_hold_s=sched_hold_s + max_extension_s, tol_s=tol_s)
            extra_c = max(0.0, hold_low - sched_hold_s)
            extra_c = math.ceil(extra_c / dt) * dt
            res = f_extended(extra_c)
            corrections.append(Correction(
                name="commercial",
                description=f"extend as though the whole process had run "
                            f"at {low_c:.1f} °C, the lowest temperature "
                            f"reached — the traditional rule",
                extra_hold_s=extra_c, hold_c=hold_c,
                f_cold_spot_min=res.f0_cold_spot_min,
                cook_mass_avg_min=res.cook_mass_avg_min,
                total_time_s=res.total_time_s, feasible=True))
        except ValueError as e:
            corrections.append(Correction(
                name="commercial", description="whole process at the "
                "lowest temperature", extra_hold_s=float("nan"),
                hold_c=hold_c, f_cold_spot_min=float("nan"),
                cook_mass_avg_min=float("nan"), total_time_s=float("nan"),
                feasible=False, note=str(e)))

    # optimum — raise the temperature for the remaining scheduled hold ------
    # Only meaningful while there is scheduled hold left after the
    # deviation clears; the remainder is re-run at a higher constant
    # temperature so the lot finishes on the original clock.
    remaining_s = hold_end - dev_end
    if remaining_s >= 2 * dt and low_c < hold_c - 0.05:
        clear_t = round(dev_end / dt) * dt
        head_dev = RetortSchedule.from_profile(
            [(t, v) for t, v in actual.breakpoints() if t < clear_t]
            + [(clear_t, actual.temperature_at(clear_t))])
        at_clear = simulate(container, product, head_dev,
                            initial_c=initial_c, options=opts)

        def f_raised(t_c: float) -> SimulationResult:
            rest = RetortSchedule.from_profile(
                [(0.0, actual.temperature_at(clear_t)), (0.0, t_c),
                 (hold_end - clear_t, t_c)])
            mid = simulate(container, product, rest, initial_c=initial_c,
                           options=opts, warm_start=at_clear)
            return simulate(container, product, tail, initial_c=initial_c,
                            options=opts, warm_start=mid)

        try:
            t_up, _ = _bisect_increasing(
                lambda x: f_raised(x).f0_cold_spot_min, target, hold_c,
                hold_c + max_raise_c, tol_x=0.05)
            res = f_raised(t_up)
            corrections.append(Correction(
                name="optimum",
                description="raise the medium for the rest of the "
                            "scheduled hold and finish on the original "
                            "clock",
                extra_hold_s=0.0, hold_c=t_up,
                f_cold_spot_min=res.f0_cold_spot_min,
                cook_mass_avg_min=res.cook_mass_avg_min,
                total_time_s=res.total_time_s, feasible=True,
                note=f"{t_up - hold_c:+.2f} °C on the medium for "
                     f"{(hold_end - clear_t) / 60:.1f} min"))
        except ValueError as e:
            corrections.append(Correction(
                name="optimum", description="raise the medium for the "
                "rest of the hold", extra_hold_s=0.0,
                hold_c=float("nan"), f_cold_spot_min=float("nan"),
                cook_mass_avg_min=float("nan"), total_time_s=float("nan"),
                feasible=False, note=str(e)))

    # -- verdict -----------------------------------------------------------
    if cooled.f0_cold_spot_min >= target:
        verdict = VERDICT_ADEQUATE
        reason = (f"the process as run still delivers "
                  f"{cooled.f0_cold_spot_min:.2f} min against a target of "
                  f"{target:.2f} min — no correction needed")
    elif process_complete:
        verdict = VERDICT_UNDER_PROCESSED
        reason = (f"the lot has cooled {target - cooled.f0_cold_spot_min:.2f} "
                  f"min short of target; nothing is left to extend. 21 CFR "
                  f"113.89: full reprocessing, or evaluation of the product "
                  f"by a competent process authority")
    else:
        prop = next((c for c in corrections if c.name == "proportional"), None)
        if prop is not None and prop.feasible:
            verdict = VERDICT_CORRECTABLE
            reason = (f"short by {target - cooled.f0_cold_spot_min:.2f} min "
                      f"at the cold spot; an added "
                      f"{prop.extra_hold_s / 60:.1f} min at {hold_c:.1f} °C "
                      f"restores the target")
        else:
            verdict = VERDICT_UNDER_PROCESSED
            reason = "the target is not reachable within the extension limit"

    return DeviationEvaluation(
        scheduled=sched_res, as_run=as_run, to_clearance=to_clear,
        target_f_min=target, deviation_low_c=low_c,
        deviation_start_s=dev_start, deviation_end_s=dev_end,
        hold_end_s=hold_end, corrections=tuple(corrections),
        verdict=verdict, reason=reason)


# ──────────────────────────────────────────────────────────────────────
# Presentation — internal, never briefing copy
# ──────────────────────────────────────────────────────────────────────
def summarize(ev: DeviationEvaluation) -> str:
    c = ev.scheduled.container
    p = ev.scheduled.product
    lines = [
        INTERNAL_BANNER,
        "",
        f"Container      {c.name}: {c.diameter_m * 1000:.1f} mm dia x "
        f"{c.height_m * 1000:.1f} mm",
        f"Product        {p.name}: alpha={p.diffusivity_m2_s * 1e7:.3f}e-7 m2/s",
        f"Scheduled      hold ends at {ev.hold_end_s / 60:.1f} min; delivers "
        f"{ev.f_scheduled_min:.2f} min at the cold spot, cooling included",
        f"Target F       {ev.target_f_min:.2f} min",
        f"Deviation      medium down to {ev.deviation_low_c:.1f} C between "
        f"{ev.deviation_start_s / 60:.1f} and {ev.deviation_end_s / 60:.1f} min",
        f"At clearance   {ev.f_delivered_at_clearance_min:.2f} min delivered "
        f"by the end of the scheduled hold",
    ]
    if ev.as_run is not None:
        lines.append(f"As run         {ev.f_as_run_min:.2f} min after cooling "
                     f"(shortfall {ev.shortfall_min:.2f} min)")
    lines += ["", f"VERDICT        {ev.verdict} — {ev.reason}", "",
              "Corrections    (a process authority chooses; this module does not)"]
    for cr in ev.corrections:
        if cr.feasible:
            lines.append(
                f"  {cr.name:<13}{cr.description}: "
                + (f"+{cr.extra_hold_s / 60:.1f} min at {cr.hold_c:.1f} C"
                   if cr.extra_hold_s > 0 else
                   (cr.note if cr.name == "optimum" else "no extension"))
                + f" -> F {cr.f_cold_spot_min:.2f} min, cook "
                f"{cr.cook_mass_avg_min:.0f} min, total "
                f"{cr.total_time_s / 60:.1f} min")
        else:
            lines.append(f"  {cr.name:<13}not feasible — {cr.note}")
    return "\n".join(lines)
