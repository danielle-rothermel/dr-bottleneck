#!/usr/bin/env bash

set -uo pipefail

MESSAGE="Say hello in one short sentence."
TEMPERATURE="0.7"
TOP_P="0.95"

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
    # shellcheck disable=SC1090
    source "${HOME}/.envrc"
fi

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
    printf 'ERROR: OPENROUTER_API_KEY is not set.\n' >&2
    exit 1
fi

PROFILES=(
    "openrouter/xiaomi/mimo-v2.5/off/v1"
    "openrouter/nvidia/llama-3.3-nemotron-super-49b-v1.5/off/v1"
    "openrouter/google/gemini-2.5-flash/off/v1"
    "openrouter/google/gemini-3-flash-preview/off/v1"
    "openrouter/google/gemini-3.1-flash-lite/off/v1"
    "openrouter/google/gemini-2.5-flash-lite/off/v1"
    "openrouter/openai/gpt-5-nano/low/v1"
    "openrouter/openai/gpt-oss-20b/low/v1"
)

failures=0

run_config() {
    local label="$1"

    printf '\n==> %s\n' "${label}"
    uv run python scripts/query_provider.py \
        --profile "${label}" \
        --message "${MESSAGE}" \
        --temperature "${TEMPERATURE}" \
        --top-p "${TOP_P}" \
        || failures=$((failures + 1))
}

for profile in "${PROFILES[@]}"; do
    run_config "${profile}"
done

if [[ "${failures}" -ne 0 ]]; then
    printf '\nERROR: %s profile(s) failed.\n' "${failures}" >&2
    exit 1
fi

printf '\nAll profiles succeeded.\n'
