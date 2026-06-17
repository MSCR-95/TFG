"""Max-2-SAT solvers based on QUBO formulations.

Pipeline for each instance:

1. **Parse CNF** — read a DIMACS file and extract variable count and clauses.
2. **Validate 2-SAT** — reject any clause that does not have exactly two literals.
3. **Build QUBO** — encode each clause as quadratic terms so that minimising
   the total QUBO energy is equivalent to maximising the number of satisfied
   clauses.  A constant offset is accumulated during this step.
4. **Build BQM** — wrap the QUBO dict in a ``dimod.BinaryQuadraticModel`` for
   sampler compatibility.
5. **Sample** — run the subclass-specific sampler to find a low-energy
   assignment.
6. **Verify** — convert sampler energy back to a satisfied-clause count via the
   stored constant, then cross-check against direct clause evaluation.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import dimod
from dimod import ExactSolver
from dwave.samplers import (
    RandomSampler,
    SimulatedAnnealingSampler,
    SteepestDescentSampler,
    TabuSampler,
)
from loguru import logger

from alglab.engine.core import Algorithm
from alglab.engine.registry import register_algorithm

from ._common import (
    assignment_payload,
    count_satisfied,
    require_clauses,
    result_payload,
)
from .parser import _parse_dimacs

_SAMPLER_SEED_MODULUS = 2**31


def _validate_2sat_clauses(clauses: list[list[int]], filename: str) -> list[tuple[int, int]]:
    clauses_2sat: list[tuple[int, int]] = []
    for idx, clause in enumerate(clauses):
        if len(clause) != 2:
            raise ValueError(
                f"{filename}: clause {idx + 1} has {len(clause)} "
                f"literal(s); this solver requires exactly 2 (Max-2-SAT)."
            )
        lit1, lit2 = clause
        clauses_2sat.append((lit1, lit2))
    return clauses_2sat


def _build_qubo(
    clauses: list[tuple[int, int]],
) -> tuple[dict[tuple[int, int], float], int]:
    Q: defaultdict[tuple[int, int], float] = defaultdict(float)
    constant = 0

    for lit1, lit2 in clauses:
        neg1, neg2 = lit1 < 0, lit2 < 0
        i, j = abs(lit1), abs(lit2)
        ii = (i, i)
        jj = (j, j)
        ij = (i, j) if i <= j else (j, i)

        if not neg1 and not neg2:
            constant += 1
            Q[ii] -= 1.0
            Q[jj] -= 1.0
            Q[ij] += 1.0
        elif neg1 and not neg2:
            Q[ii] += 1.0
            Q[ij] -= 1.0
        elif not neg1 and neg2:
            Q[jj] += 1.0
            Q[ij] -= 1.0
        else:
            Q[ij] += 1.0

    return dict(Q), constant


def _sampler_seed(seed: int) -> int:
    """Adapt engine-level deterministic seeds to sampler APIs that require uint32."""
    return seed % _SAMPLER_SEED_MODULUS


@dataclass(frozen=True)
class MaxSATQUBOContext:
    """Immutable context produced by ``before_run`` and consumed by ``run``/``after_run``.

    Attributes:
        source_name: Filename of the DIMACS instance, used in log messages.
        num_vars: Number of Boolean variables declared in the DIMACS header.
        clauses_2sat: Validated list of clauses, each represented as a pair of
            signed integers (positive = positive literal, negative = negated).
        bqm: Binary quadratic model built from the QUBO encoding, ready for
            any ``dimod``-compatible sampler.
        constant: Energy offset accumulated while constructing the QUBO.
            When a clause contributes a constant term rather than a quadratic
            one, that value is stored here so that
            ``num_unsatisfied = round(qubo_energy + constant)`` correctly
            recovers the unsatisfied-clause count from the sampler's raw energy.
    """

    source_name: str
    num_vars: int
    clauses_2sat: list[tuple[int, int]]
    bqm: dimod.BinaryQuadraticModel
    constant: int


class MaxSATQUBOBase(Algorithm[MaxSATQUBOContext]):
    """Base class for all Max-2-SAT QUBO solvers via dwave.samplers.

    Each concrete subclass sets ``sampler_cls`` to a specific ``dimod``-compatible
    sampler.  The lifecycle methods handle CNF parsing, QUBO construction,
    sampling, and result verification.

    Attributes:
        sampler_cls: Sampler class instantiated by ``run``; set by each subclass.

    Args:
        num_reads: Number of reads (samples) passed to samplers that accept
            the ``num_reads`` parameter.  Ignored for samplers that do not
            expose it (e.g. ``ExactSolver``).
        include_assignment: When ``True``, the full Boolean variable assignment
            of the best sample is included in the returned payload under the
            ``assignment`` key.
    """

    sampler_cls: ClassVar[type[Any] | None] = None

    def __init__(
        self,
        num_reads: int = 100,
        include_assignment: bool = False,
    ) -> None:
        """Initialise the QUBO solver.

        Args:
            num_reads: Number of samples requested from compatible samplers.
            include_assignment: When ``True``, include the full best-sample
                assignment in the result payload.
        """
        self.num_reads = num_reads
        self.include_assignment = include_assignment

    def before_run(self, file_path: Path) -> MaxSATQUBOContext:
        """Parse the DIMACS file and build the BQM that will be sampled.

        Reads the CNF instance, validates that every clause has exactly two
        literals, constructs the QUBO encoding, and wraps it in a
        ``dimod.BinaryQuadraticModel``.

        Args:
            file_path: Path to a DIMACS-format Max-2-SAT instance.

        Returns:
            A frozen context containing the BQM and metadata needed by the
            remaining lifecycle methods.

        Raises:
            ValueError: If any clause does not have exactly two literals.
        """
        text = file_path.read_text(encoding="utf-8")
        num_vars, raw_clauses = _parse_dimacs(text)
        require_clauses(raw_clauses, file_path.name)

        clauses_2sat = _validate_2sat_clauses(raw_clauses, file_path.name)
        Q, constant = _build_qubo(clauses_2sat)
        bqm = dimod.BinaryQuadraticModel.from_qubo(Q)

        logger.debug(
            "QUBO ready for {}: {} vars, {} clauses",
            file_path.name,
            num_vars,
            len(clauses_2sat),
        )

        return MaxSATQUBOContext(
            source_name=file_path.name,
            num_vars=num_vars,
            clauses_2sat=clauses_2sat,
            bqm=bqm,
            constant=constant,
        )

    def run(self, context: MaxSATQUBOContext, *, seed: int | None = None) -> dict[str, Any]:
        """Sample the BQM and return the best energy and raw variable assignment.

        Instantiates ``sampler_cls``, forwards ``num_reads`` and ``seed`` only
        when the sampler's parameter set advertises those keys, then returns the
        lowest-energy sample found.

        Args:
            context: Context produced by ``before_run``.
            seed: Optional deterministic seed forwarded to the sampler after
                reduction modulo ``2**31`` for uint32 compatibility.

        Returns:
            Intermediate payload with ``qubo_energy`` (rounded to six decimal
            places) and ``_raw_sample`` (the variable-to-spin mapping of the
            best solution).  The ``_raw_sample`` key is consumed and removed by
            ``after_run``.

        Raises:
            NotImplementedError: If the subclass has not set ``sampler_cls``.
        """
        if self.sampler_cls is None:
            raise NotImplementedError("QUBO solvers must define sampler_cls")

        sampler = self.sampler_cls()
        sampler_params = getattr(sampler, "parameters", {})
        sample_kwargs: dict[str, Any] = {}
        if "num_reads" in sampler_params:
            sample_kwargs["num_reads"] = self.num_reads
        if seed is not None and "seed" in sampler_params:
            sample_kwargs["seed"] = _sampler_seed(seed)

        best = sampler.sample(context.bqm, **sample_kwargs).first
        return {
            "qubo_energy": round(best.energy, 6),
            "_raw_sample": best.sample,
        }

    def after_run(
        self,
        context: MaxSATQUBOContext,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Convert QUBO energy to a satisfied-clause count and verify it.

        Recovers the number of satisfied clauses from the sampler's raw energy
        via ``inferred_sat = num_clauses - round(qubo_energy + constant)``, then
        independently evaluates every clause against the best sample.  If the
        two counts differ a warning is logged and the directly evaluated count
        is used as the authoritative value.

        Args:
            context: Context produced by ``before_run``.
            payload: Intermediate payload from ``run``.  The ``qubo_energy`` and
                ``_raw_sample`` keys are consumed here and must not be present in
                the final result.

        Returns:
            Final result payload containing ``satisfaction_ratio``,
            ``num_vars``, ``num_clauses``, ``satisfied_clauses``, and
            ``verification_mismatch``.  Also includes ``assignment`` when
            ``include_assignment`` was set at construction time.
        """
        sample = payload.pop("_raw_sample")
        qubo_energy = payload.pop("qubo_energy")
        num_clauses = len(context.clauses_2sat)

        inferred_unsat = max(0, min(num_clauses, round(qubo_energy + context.constant)))
        inferred_sat = num_clauses - inferred_unsat

        actual_sat = count_satisfied(context.clauses_2sat, sample)
        mismatch = actual_sat != inferred_sat

        if mismatch:
            logger.warning(
                "{}: Mismatch detected (Energy-inferred: {} vs Actual: {}). Using actual.",
                context.source_name,
                inferred_sat,
                actual_sat,
            )

        payload.update(
            result_payload(
                num_vars=context.num_vars,
                num_clauses=num_clauses,
                satisfied_clauses=actual_sat,
                verification_mismatch=mismatch,
            )
        )

        if self.include_assignment:
            assignment = {k: bool(v) for k, v in sample.items()}
            payload["assignment"] = assignment_payload(assignment)

        return payload


@register_algorithm("maxsat_qubo_exact")
class MaxSATQUBOExactAlgorithm(MaxSATQUBOBase):
    """Solves Max-2-SAT via QUBO with an exact brute-force solver."""

    sampler_cls = ExactSolver


@register_algorithm("maxsat_qubo_rs")
class MaxSATQUBORSAlgorithm(MaxSATQUBOBase):
    """Solves Max-2-SAT via QUBO with random sampling."""

    sampler_cls = RandomSampler


@register_algorithm("maxsat_qubo_sa")
class MaxSATQUBOSAAlgorithm(MaxSATQUBOBase):
    """Solves Max-2-SAT via QUBO with simulated annealing."""

    sampler_cls = SimulatedAnnealingSampler


@register_algorithm("maxsat_qubo_sds")
class MaxSATQUBOSDSAlgorithm(MaxSATQUBOBase):
    """Solves Max-2-SAT via QUBO with steepest descent."""

    sampler_cls = SteepestDescentSampler


@register_algorithm("maxsat_qubo_ts")
class MaxSATQUBOTSAlgorithm(MaxSATQUBOBase):
    """Solves Max-2-SAT via QUBO with tabu search."""

    sampler_cls = TabuSampler
