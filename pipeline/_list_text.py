"""A list is not text (2026-10-01).

When a model answers a text field with a JSON array, `str(val)` writes the
Python repr into the register:

    Reason  "['Contamination of product with salmonella',
              'Contamination of product with salmonella']"

Two FSA rows were published that way (PRIN-47-2026 update 1, PRIN-28-2026
update 1) and one sat in Rejected (PRIN-41-2026). All came through
`_safe()` in an extractor, which had four identical copies.

`as_text()` is the one place that turns a value into cell text: a list or
tuple becomes its distinct non-empty items joined by "; ", and a string that
is exactly a Python/JSON list of strings is parsed and joined the same way.
Anything else is returned unchanged. Pure, no I/O.
"""
from __future__ import annotations

import ast
import json
import re
from typing import Any

_LISTISH = re.compile(r"""^\s*\[\s*(?:(['"]).*\1\s*,?\s*)*\]\s*$""", re.S)


def _join(items) -> str:
    out, seen = [], set()
    for x in items:
        s = str(x).strip() if x is not None else ""
        k = s.casefold()
        if s and k not in seen:
            seen.add(k)
            out.append(s)
    return "; ".join(out)


def as_text(val: Any) -> Any:
    if isinstance(val, (list, tuple)):
        return _join(val)
    if isinstance(val, str) and val.lstrip().startswith("[") and _LISTISH.match(val):
        for parse in (ast.literal_eval, json.loads):
            try:
                got = parse(val.strip())
            except Exception:
                continue
            if isinstance(got, list) and all(isinstance(x, str) for x in got):
                return _join(got)
    return val
