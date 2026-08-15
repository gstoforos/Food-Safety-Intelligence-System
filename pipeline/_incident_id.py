"""Incident identity — one event, many regulator notices.

WHY THIS IS SEPARATE FROM _outbreak_id.py
==========================================
_outbreak_id solves the same SHAPE of problem for OUTBREAKS: several rows,
one human-illness cluster. This module solves it for RECALL INCIDENTS,
where there is no illness at all.

They must not be merged, and the 2026-08-15 case shows why. E.Leclerc
Dinan (Côtes-d'Armor) had a suspected refrigeration failure. DGCCRF filed
TWENTY fiches for it — 23187, 23188, 23189, 23190, 23191, 23196-23200,
23202-23211 — one per supplier whose chilled stock sat in that store:
Labeyrie, Delpierre, Marque Repère, Mowi, Delpeyrat, Pêcheries Sétoises,
Cap Océan, Baltic, Breizh Saveurs, Biovillage. Every fiche carries the
identical motif, "suspicion de rupture de la chaine du froid", the same
distributeur (Leclerc Dinan) and the same département. Fiche 23196's own
entreprise field reads "DINAN DISTRIBUTION CENTRE LECLERC CENTRE E.
LECLERC DINAN" — the store itself.

Counted as rows, that single broken chiller would have TRIPLED the week
(W34 had 10 rows) and made Listeria the dominant pathogen of the week on
the strength of one refrigeration fault. Counted as one incident, the
week reads honestly and all twenty products stay individually searchable.

WHY NOT JUST SET Outbreak=1 AND REUSE THE OUTBREAK MACHINERY
-------------------------------------------------------------
Because there is no outbreak. No illnesses were reported, no organism was
isolated from any product — the recall is precautionary, triggered by
temperature, and the Listeria in the risques field is the POTENTIAL
consequence of that temperature excursion, not a finding. Flagging it as
an outbreak to get the deduplication for free would put a fictional
cluster in the outbreak KPI, which is exactly the error this register has
spent weeks removing (the AGES Radar row, the Coaticook row, the EFSA
row). Outbreak stays 0 on all twenty.

IDENTITY
--------
There is no equivalent of an investigation slug here — a national
regulator does not issue an "incident id" for a shop's failed chiller. So
identity is an OPERATOR OVERRIDE written into Notes and nothing else:

    [incident:fr:leclerc-dinan-2026-08-15]

Nothing is inferred. A row with no tag is its own incident, which means
every historical week keeps exactly the count it already has and this
module can only ever REDUCE a count where a human has said two notices
are one event.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional

_NOTES_RE = re.compile(r"\[incident:\s*([A-Za-z0-9:_\-\.]+)\s*\]")


def derive(row: Dict[str, Any]) -> Optional[str]:
    """Return the operator-assigned incident id, or None.

    Deliberately has no inference path. Compare _outbreak_id.derive, which
    falls back to a pathogen+month key at LOW confidence — that fallback
    exists there because investigation slugs are sometimes missing from a
    URL that still clearly belongs to a known outbreak. Here there is no
    equivalent signal, and guessing that two recalls on the same day in
    the same département are "one incident" would silently merge genuinely
    separate events. Absence of a tag means absence of a claim.
    """
    m = _NOTES_RE.search(str(row.get("Notes", "") or ""))
    return m.group(1).lower() if m else None


def count_incidents(rows: Iterable[Dict[str, Any]]) -> int:
    """Distinct incidents: untagged rows count individually, tagged rows
    collapse to one per id.

    count_incidents(rows) <= len(rows), with equality when nothing is
    tagged — so wiring this in cannot change any historical figure.
    """
    seen, untagged = set(), 0
    for r in rows:
        iid = derive(r)
        if iid:
            seen.add(iid)
        else:
            untagged += 1
    return len(seen) + untagged


def group_sizes(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """{incident_id: row_count} for the tagged rows only. For reporting
    '20 notices, 1 incident' rather than hiding the collapse."""
    out: Dict[str, int] = {}
    for r in rows:
        iid = derive(r)
        if iid:
            out[iid] = out.get(iid, 0) + 1
    return out
