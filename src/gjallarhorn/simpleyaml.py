from __future__ import annotations

from collections import OrderedDict


class YamlError(ValueError):
    """Raised when parsing unsupported YAML."""


def loads(text: str) -> object:
    lines = _tokenize(text)
    if not lines:
        return OrderedDict()
    value, next_index = _parse_block(lines, 0, lines[0].indent)
    if next_index != len(lines):
        raise YamlError(
            f"Unexpected trailing content on line {lines[next_index].lineno}"
        )
    return value


def dumps(value: object) -> str:
    rendered = _dump_value(value, 0)
    if not rendered.endswith("\n"):
        rendered += "\n"
    return rendered


class _Line:
    __slots__ = ("lineno", "indent", "content")

    def __init__(self, lineno: int, indent: int, content: str) -> None:
        self.lineno = lineno
        self.indent = indent
        self.content = content


def _tokenize(text: str) -> list[_Line]:
    result: list[_Line] = []
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        if "\t" in raw_line:
            raise YamlError(f"Tabs are not supported (line {lineno})")
        stripped = raw_line.lstrip(" ")
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(stripped)
        if indent % 2 != 0:
            raise YamlError(f"Indentation must use 2-space increments (line {lineno})")
        result.append(_Line(lineno, indent, stripped.rstrip()))
    return result


def _parse_block(lines: list[_Line], index: int, indent: int) -> tuple[object, int]:
    if lines[index].indent != indent:
        raise YamlError(f"Unexpected indentation on line {lines[index].lineno}")
    if lines[index].content.startswith("-"):
        return _parse_list(lines, index, indent)
    return _parse_mapping(lines, index, indent)


def _parse_mapping(
    lines: list[_Line], index: int, indent: int
) -> tuple[OrderedDict[str, object], int]:
    mapping: OrderedDict[str, object] = OrderedDict()
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent > indent:
            raise YamlError(f"Unexpected indentation on line {line.lineno}")
        if line.content.startswith("-"):
            break
        key, raw_value = _split_mapping(line)
        if key in mapping:
            raise YamlError(f"Duplicate key '{key}' on line {line.lineno}")
        index += 1
        if raw_value == "":
            if index < len(lines) and lines[index].indent > indent:
                value, index = _parse_block(lines, index, lines[index].indent)
            else:
                value = None
        else:
            value = _parse_scalar(raw_value, line.lineno)
        mapping[key] = value
    return mapping, index


def _parse_list(
    lines: list[_Line], index: int, indent: int
) -> tuple[list[object], int]:
    values: list[object] = []
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent > indent:
            raise YamlError(f"Unexpected indentation on line {line.lineno}")
        if not line.content.startswith("-"):
            break
        item_text = line.content[1:].lstrip(" ")
        index += 1
        if item_text == "":
            if index < len(lines) and lines[index].indent > indent:
                value, index = _parse_block(lines, index, lines[index].indent)
            else:
                value = None
        elif _looks_like_inline_mapping(item_text):
            nested_line = _Line(line.lineno, indent + 2, item_text)
            value, _ = _parse_mapping([nested_line], 0, indent + 2)
        else:
            value = _parse_scalar(item_text, line.lineno)
        values.append(value)
    return values, index


def _split_mapping(line: _Line) -> tuple[str, str]:
    content = line.content
    in_single = False
    in_double = False
    for idx, char in enumerate(content):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == ":" and not in_single and not in_double:
            key = content[:idx].strip()
            if not key:
                raise YamlError(f"Missing mapping key on line {line.lineno}")
            return key, content[idx + 1 :].strip()
    raise YamlError(f"Invalid mapping syntax on line {line.lineno}")


def _looks_like_inline_mapping(value: str) -> bool:
    in_single = False
    in_double = False
    for idx, char in enumerate(value):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == ":" and not in_single and not in_double:
            return idx > 0
    return False


def _parse_scalar(raw_value: str, lineno: int) -> object:
    value = _strip_comment(raw_value).strip()
    if value == "":
        return ""
    if value == "[]":
        return []
    if value == "{}":
        return OrderedDict()
    if value in {"true", "false"}:
        return value == "true"
    if value in {"null", "Null", "NULL", "~"}:
        return None
    if value.startswith('"'):
        if not value.endswith('"') or len(value) == 1:
            raise YamlError(f"Malformed double-quoted string on line {lineno}")
        inner = value[1:-1]
        return bytes(inner, "utf-8").decode("unicode_escape")
    if value.startswith("'"):
        if not value.endswith("'") or len(value) == 1:
            raise YamlError(f"Malformed single-quoted string on line {lineno}")
        inner = value[1:-1]
        return inner.replace("''", "'")
    if value.startswith(("[", "{", "|", ">")):
        raise YamlError(f"Unsupported YAML value syntax on line {lineno}")
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def _strip_comment(value: str) -> str:
    in_single = False
    in_double = False
    for idx, char in enumerate(value):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif (
            char == "#"
            and not in_single
            and not in_double
            and (idx == 0 or value[idx - 1].isspace())
        ):
            return value[:idx].rstrip()
    return value


def _dump_value(value: object, indent: int) -> str:
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines: list[str] = []
        for key, nested_value in value.items():
            prefix = " " * indent + f"{key}:"
            if isinstance(nested_value, (dict, list)) and nested_value:
                lines.append(prefix)
                lines.append(_dump_value(nested_value, indent + 2))
            elif isinstance(nested_value, list) and not nested_value:
                lines.append(prefix + " []")
            elif isinstance(nested_value, dict) and not nested_value:
                lines.append(prefix + " {}")
            elif nested_value is None:
                lines.append(prefix)
            else:
                lines.append(prefix + " " + _dump_scalar(nested_value))
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        lines = []
        for item in value:
            prefix = " " * indent + "-"
            if isinstance(item, (dict, list)) and item:
                lines.append(prefix)
                lines.append(_dump_value(item, indent + 2))
            elif item is None:
                lines.append(prefix)
            else:
                lines.append(prefix + " " + _dump_scalar(item))
        return "\n".join(lines)
    return " " * indent + _dump_scalar(value)


def _dump_scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, str):
        raise TypeError(f"Unsupported YAML scalar type: {type(value)!r}")
    if value == "":
        return "''"
    if _needs_quotes(value):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return value


def _needs_quotes(value: str) -> bool:
    special_prefixes = (
        "-",
        "?",
        ":",
        "@",
        "`",
        "{",
        "}",
        "[",
        "]",
        ",",
        "&",
        "*",
        "!",
        "|",
        ">",
        "%",
        "#",
    )
    if value.startswith(special_prefixes):
        return True
    if value in {"true", "false", "null", "Null", "NULL", "~"}:
        return True
    if value.lstrip("-").isdigit():
        return True
    if value.strip() != value:
        return True
    if any(ch in value for ch in ("\n", "\r", "\t")):
        return True
    return bool("# " in value or value.endswith("#"))
