from __future__ import annotations

import ast
from typing import NamedTuple

from dr_code.models.humaneval import HumanEvalPlusTask


class DecoderOnlyContext(NamedTuple):
    signature: str
    encoded_description: str


def decoder_only_context(task: HumanEvalPlusTask) -> DecoderOnlyContext:
    tree = ast.parse(task.prompt)
    function = _find_function(tree, task.entry_point)
    source_lines = task.prompt.splitlines()
    import_lines = _leading_import_lines(tree, source_lines)
    signature_lines = _function_header_lines(function, source_lines)
    docstring = ast.get_docstring(function, clean=False) or task.prompt
    return DecoderOnlyContext(
        signature="\n".join([*import_lines, *signature_lines]).strip(),
        encoded_description=docstring.strip(),
    )


def _find_function(tree: ast.Module, entry_point: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == entry_point:
            return node
    msg = f"Prompt does not define expected entry point {entry_point!r}."
    raise ValueError(msg)


def _leading_import_lines(
    tree: ast.Module,
    source_lines: list[str],
) -> list[str]:
    lines: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.Import | ast.ImportFrom):
            continue
        lines.extend(source_lines[node.lineno - 1 : node.end_lineno])
    return lines


def _function_header_lines(
    function: ast.FunctionDef,
    source_lines: list[str],
) -> list[str]:
    if not function.body:
        msg = f"Function {function.name!r} has no body."
        raise ValueError(msg)
    first_body = function.body[0]
    return source_lines[function.lineno - 1 : first_body.lineno - 1]
