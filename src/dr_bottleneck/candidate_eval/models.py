from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1
REQUEST_PAYLOAD_KEY = "candidate_eval_request"
CANDIDATE_EVAL_STAGE = "candidate_eval"
DEFAULT_REQUEST_QUEUE = "bottleneck.candidate_eval.requests"


class CandidateEvalPhase(StrEnum):
    DECODER_FORMAT = "decoder_format"
    DECODER_CORRECTNESS = "decoder_correctness"
    ENCODER_FULL_PATH = "encoder_full_path"


class CandidateMetricTarget(StrEnum):
    AST_PARSE_RATE = "ast_parse_rate"
    TEST_PASS_RATE = "test_pass_rate"
    CORRECTNESS_THEN_COMPRESSION = "correctness_then_compression"


class CandidateVariant(StrEnum):
    SIGNATURE_SIDE_CHANNEL = "signature_side_channel"
    DESCRIPTION_ONLY = "description_only"


class CandidateExecutionMode(StrEnum):
    IN_PROCESS = "in_process"
    DETACHED = "detached"


class CandidateEvalStatus(StrEnum):
    COMPLETE = "complete"
    FAILED = "failed"


class FailureBucket(StrEnum):
    PASSED = "passed"
    INVALID_PYTHON = "invalid_python"
    MISSING_ENTRY_POINT = "missing_entry_point"
    INCOMPATIBLE_SIGNATURE = "incompatible_signature"
    RUNTIME_ERROR = "runtime_error"
    FAILED_ASSERTIONS = "failed_assertions"
    SKIPPED = "skipped"
    INTERNAL_ERROR = "internal_error"


class CandidateEvalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    optimizer_run_id: str
    candidate_id: str
    phase: CandidateEvalPhase
    metric_target: CandidateMetricTarget
    variant: CandidateVariant
    decoder_template_text: str
    encoder_template_text: str | None = None
    slot_values: dict[str, str] = Field(default_factory=dict)
    task_ids: list[str]
    lane_ids: list[str]
    budgets: list[int]
    repeats: int = 1
    bottleneck_workers: str = "decode=1"
    code_eval_workers: str = "parse=1,test=1"
    execution_mode: CandidateExecutionMode = CandidateExecutionMode.IN_PROCESS
    completion_timeout_seconds: float = 3600.0
    result_queue: str

    @property
    def is_decoder_only(self) -> bool:
        return self.phase in {
            CandidateEvalPhase.DECODER_FORMAT,
            CandidateEvalPhase.DECODER_CORRECTNESS,
        }


class CandidateAggregateMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attempt_count: int = 0
    parse_success_count: int = 0
    tests_ran_count: int = 0
    all_tests_passed_count: int = 0
    parse_rate: float = 0.0
    tests_ran_rate: float = 0.0
    all_tests_passed_rate: float = 0.0
    mean_test_pass_rate: float | None = None
    total_decoder_input_bytes: int = 0
    total_compressed_decoder_input_bytes: int = 0
    mean_decoder_input_bytes: float = 0.0
    mean_compressed_decoder_input_bytes: float = 0.0


class CandidateExampleResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    lane: str
    budget: int
    repeat: int
    sample_id: str
    parse_success: bool
    entry_point_exists: bool
    signature_compatible: bool
    tests_ran: bool
    all_tests_passed: bool | None
    test_pass_rate: float | None
    failure_bucket: FailureBucket
    feedback: str
    decoder_input_bytes: int
    compressed_decoder_input_bytes: int


class CandidateEvalResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    UNKNOWN_VALUE: ClassVar[str] = "unknown"

    schema_version: int = SCHEMA_VERSION
    optimizer_run_id: str
    candidate_id: str
    status: CandidateEvalStatus
    error_type: str | None = None
    error_message: str | None = None
    bottleneck_run_id: str | None = None
    code_eval_run_id: str | None = None
    aggregate_metrics: CandidateAggregateMetrics = Field(
        default_factory=CandidateAggregateMetrics
    )
    examples: list[CandidateExampleResult] = Field(default_factory=list)
    provenance_refs: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def failed(
        cls,
        *,
        optimizer_run_id: str,
        candidate_id: str,
        error: Exception,
        bottleneck_run_id: str | None = None,
        code_eval_run_id: str | None = None,
    ) -> CandidateEvalResult:
        return cls(
            optimizer_run_id=optimizer_run_id,
            candidate_id=candidate_id,
            status=CandidateEvalStatus.FAILED,
            error_type=type(error).__name__,
            error_message=str(error),
            bottleneck_run_id=bottleneck_run_id,
            code_eval_run_id=code_eval_run_id,
        )
