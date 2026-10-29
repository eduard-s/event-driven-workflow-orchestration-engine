# Template syntax shared by API validation and orchestrator resolution.

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from shared.errors import DomainError

_NODE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}")
_FIELD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_INDEX = re.compile(r"(?:0|[1-9][0-9]*)\]")
_DECODER = json.JSONDecoder()


@dataclass(frozen=True)
class Reference:
    # Keep an immutable output path so API validation and orchestrator lookup share one parsed reference.
    #
    # Args:
    #     node_id: Ancestor node whose output supplies the value.
    #     steps: Object keys and nonnegative array indexes after the node ID.
    #
    # Returns:
    #     A reference instance with its node ID and path steps.

    node_id: str
    steps: tuple[str | int, ...]


def _syntax(path: str) -> DomainError:
    # Create one template error shape so all grammar failures report the same code and source path.
    #
    # Args:
    #     path: JSON path of the field that contains the template.
    #
    # Returns:
    #     A DomainError with status 422 and code INVALID_TEMPLATE. This function does not raise it.

    return DomainError(422, "INVALID_TEMPLATE", "The template reference has invalid syntax.", path)


def _reference(expression: str, path: str) -> Reference:
    # Parse one expression so output lookup receives explicit keys and indexes instead of executable text.
    #
    # Args:
    #     expression: Text between template delimiters, with optional outer whitespace.
    #     path: Source JSON path for error reports.
    #
    # Returns:
    #     The referenced node ID and its ordered field or array steps.
    #
    # Raises:
    #     DomainError: The node ID, field, quoted key, index, or delimiter syntax is invalid.

    expression = expression.strip()
    match = _NODE.match(expression)
    if match is None:
        raise _syntax(path)
    node_id = match.group()
    position = match.end()
    steps: list[str | int] = []
    while position < len(expression):
        if expression[position] == ".":
            field = _FIELD.match(expression, position + 1)
            if field is None:
                raise _syntax(path)
            steps.append(field.group())
            position = field.end()
        elif expression[position] == "[":
            position += 1
            if expression[position : position + 1] == '"':
                try:
                    key, position = _DECODER.raw_decode(expression, position)
                except ValueError as error:
                    raise _syntax(path) from error
                if not isinstance(key, str) or any(0xD800 <= ord(char) <= 0xDFFF for char in key):
                    raise _syntax(path)
                if expression[position : position + 1] != "]":
                    raise _syntax(path)
                steps.append(key)
                position += 1
            else:
                index = _INDEX.match(expression, position)
                if index is None:
                    raise _syntax(path)
                try:
                    steps.append(int(index.group()[:-1]))
                except ValueError as error:
                    raise _syntax(path) from error
                position = index.end()
        else:
            raise _syntax(path)
    return Reference(node_id, tuple(steps))


def _reference_end(text: str, start: int, path: str) -> int:
    # Find the closing delimiter outside quoted keys so a JSON key can contain delimiter text.
    #
    # Args:
    #     text: Complete template text.
    #     start: Character offset after the opening delimiter.
    #     path: Source JSON path for error reports.
    #
    # Returns:
    #     Character offset of the closing delimiter.
    #
    # Raises:
    #     DomainError: No closing delimiter exists outside a quoted key.
    position = start
    quoted = False
    escaped = False
    while position < len(text):
        character = text[position]
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif text.startswith("}}", position):
            return position
        position += 1
    raise _syntax(path)


def parse_template(text: str, path: str = "$") -> list[str | Reference]:
    # Split text into literals and references so validation and resolution obey the same escape and delimiter rules.
    #
    # Args:
    #     text: Literal or templated text. Four repeated braces escape a literal delimiter.
    #     path: Source JSON path for error reports.
    #
    # Returns:
    #     Literal strings and Reference objects in text order. Empty text returns an empty list.
    #
    # Raises:
    #     DomainError: A reference or delimiter has invalid syntax.
    if "{{" not in text and "}}" not in text:
        return [text] if text else []
    tokens: list[str | Reference] = []
    literals: list[str] = []
    position = 0
    literal_start = 0
    while position < len(text):
        escape = text[position : position + 4]
        if escape in {"{{{{", "}}}}"}:
            literals.extend((text[literal_start:position], escape[:2]))
            position += 4
            literal_start = position
        elif text.startswith("{{", position):
            literals.append(text[literal_start:position])
            literal = "".join(literals)
            if literal:
                tokens.append(literal)
            literals = []
            start = position + 2
            position = _reference_end(text, start, path)
            tokens.append(_reference(text[start:position], path))
            position += 2
            literal_start = position
        elif text.startswith("}}", position):
            raise _syntax(path)
        else:
            position += 1
    literals.append(text[literal_start:])
    literal = "".join(literals)
    if literal:
        tokens.append(literal)
    return tokens


def iter_references(value: Any, path: str = "$") -> Iterator[tuple[Reference, str]]:
    # Visit references in JSON values so ancestor validation can examine nested data while object keys stay literal.
    #
    # Args:
    #     value: Native JSON value to inspect.
    #     path: JSON path for the starting value.
    #
    # Yields:
    #     Each parsed Reference and the JSON path of its containing string.
    #
    # Raises:
    #     DomainError: A string contains invalid template syntax.
    pending: list[tuple[Any, str]] = [(value, path)]
    while pending:
        current, current_path = pending.pop()
        if isinstance(current, str):
            for token in parse_template(current, current_path):
                if isinstance(token, Reference):
                    yield token, current_path
        elif isinstance(current, list):
            pending.extend((item, f"{current_path}[{index}]") for index, item in enumerate(current))
        elif isinstance(current, dict):
            pending.extend((item, f"{current_path}.{key}") for key, item in current.items())
