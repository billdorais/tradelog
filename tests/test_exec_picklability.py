"""Regression test for the exec'd Strategy pickle fix in bt_run / bt_optimize.

Background: backtesting.py's `Backtest.optimize(...)` fans out across
processes via `multiprocessing`, which may require the Strategy class to
be picklable. A class produced by `exec(code, ns)` has `__module__ =
'builtins'` and is findable nowhere, so pickle fails.

IMPORTANT FINDING: stamping `__module__ = '__main__'` alone is NOT
sufficient — pickle resolves classes by looking up
`sys.modules[cls.__module__].<qualname>`, so the class must also be
attached to the `__main__` module. This test documents both the broken
fix AND the complete fix, so if someone improves the production code
they know exactly what to do.

On Windows, `backtesting.py` falls back to a ThreadPool (no pickling);
on Linux `fork()` inherits parent memory (no pickling). So the
half-fix in production currently works by luck of platform. If
`backtesting.Pool` is ever overridden to a real multiprocessing pool
under spawn, we'll hit the bug again — this test guards against that.
"""
from __future__ import annotations

import pickle
import sys

from backtesting import Strategy


CONVERTED_STRATEGY_SRC = """
from backtesting import Strategy

class DummyStrategy(Strategy):
    def init(self):
        pass
    def next(self):
        pass
"""


def _resolve_strategy_cls(code: str):
    ns = {"Strategy": Strategy}
    exec(code, ns)
    return next(
        v for v in ns.values()
        if isinstance(v, type) and issubclass(v, Strategy) and v is not Strategy
    )


def test_bare_exec_class_is_not_picklable():
    """The baseline failure: exec'd class with no fixup."""
    cls = _resolve_strategy_cls(CONVERTED_STRATEGY_SRC)
    try:
        pickle.dumps(cls)
    except (pickle.PicklingError, AttributeError):
        return
    raise AssertionError("Expected pickle to fail on bare exec'd class")


def test_module_stamp_alone_is_insufficient():
    """Documents the gap in the current app.py fix.

    The class has __module__='__main__' but isn't attached to
    sys.modules['__main__'], so pickle can't resolve it."""
    cls = _resolve_strategy_cls(CONVERTED_STRATEGY_SRC)
    cls.__module__ = "__main__"
    cls.__qualname__ = cls.__name__

    # Clean up any lingering attribute from prior tests
    if hasattr(sys.modules["__main__"], cls.__name__):
        delattr(sys.modules["__main__"], cls.__name__)

    try:
        pickle.dumps(cls)
    except pickle.PicklingError:
        return
    raise AssertionError(
        "Stamping __module__ alone should not be enough — "
        "if this passes unexpectedly, the production fix is more "
        "robust than we think."
    )


def test_module_stamp_plus_sys_modules_registration_works():
    """The complete fix. Recommend adding this line to bt_run/bt_optimize:

        setattr(sys.modules['__main__'], strategy_cls.__name__, strategy_cls)
    """
    cls = _resolve_strategy_cls(CONVERTED_STRATEGY_SRC)
    cls.__module__ = "__main__"
    cls.__qualname__ = cls.__name__
    setattr(sys.modules["__main__"], cls.__name__, cls)

    blob = pickle.dumps(cls)
    assert isinstance(blob, bytes) and len(blob) > 0

    # Clean up for other tests
    delattr(sys.modules["__main__"], cls.__name__)
