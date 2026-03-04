import json
import pathlib
import sys
import time
import unittest
from dataclasses import asdict, dataclass

# Ensure imports like `analyzer.*` work when this script is executed directly.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass
class Metrics:
    total: int
    passed: int
    failed: int
    errors: int
    skipped: int
    success_rate: float
    duration_seconds: float


class TimedTextTestResult(unittest.TextTestResult):
    def startTest(self, test):
        self._test_start_time = time.perf_counter()
        super().startTest(test)

    def stopTest(self, test):
        elapsed = time.perf_counter() - self._test_start_time
        if not hasattr(self, "test_durations"):
            self.test_durations = []
        self.test_durations.append((str(test), elapsed))
        super().stopTest(test)


class TimedTextTestRunner(unittest.TextTestRunner):
    resultclass = TimedTextTestResult


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_*.py")

    run_start = time.perf_counter()
    runner = TimedTextTestRunner(verbosity=2)
    result = runner.run(suite)
    duration = time.perf_counter() - run_start

    total = result.testsRun
    failed = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped)
    passed = total - failed - errors - skipped
    success_rate = (passed / total * 100.0) if total else 0.0

    metrics = Metrics(
        total=total,
        passed=passed,
        failed=failed,
        errors=errors,
        skipped=skipped,
        success_rate=success_rate,
        duration_seconds=duration,
    )

    print("\n=== TEST METRICS ===")
    print(json.dumps(asdict(metrics), ensure_ascii=False, indent=2))

    durations = getattr(result, "test_durations", [])
    if durations:
        print("\n=== SLOWEST TESTS (top 5) ===")
        for name, sec in sorted(durations, key=lambda x: x[1], reverse=True)[:5]:
            print(f"{sec:.4f}s  {name}")

    raise SystemExit(0 if result.wasSuccessful() else 1)
