"""Convert raw knowledge to UTF-8 Markdown without changing source files.

Run with the project's Python. Uses the standard library and the installed
charset_normalizer only as a fallback for non-Unicode source encodings.
Original filenames are retained: input.json -> input.json.md. ZIP members
are converted under input.zip/, alongside an input.zip.md member manifest.
"""

import argparse
import base64
import csv
import hashlib
import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter


INVALID = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\ufffd\ufffe\uffff]")
TEXT_TYPES = {"", ".md", ".txt", ".log", ".md5", ".sql", ".yaml", ".yml"}
IMAGES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}
W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"


def encoding_for(path):
    with path.open("rb") as stream:
        sample = stream.read(131072)
    if sample.startswith((b"\xff\xfe\x00\x00", b"\x00\x00\xfe\xff")):
        return "utf-32"
    if sample.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if b"\x00" in sample:
        raise ValueError("Binary content or UTF-16 without a byte-order mark")
    # Validate the full file so a multibyte character at a sample boundary
    # cannot cause a wrong encoding guess.
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            with path.open(encoding=encoding) as stream:
                while stream.read(1048576):
                    pass
            return encoding
        except UnicodeError:
            pass
    from charset_normalizer import from_bytes
    guess = from_bytes(sample).best()
    if guess is None or guess.percent_chaos > 10:
        raise UnicodeError("Unable to determine a reliable source encoding")
    return guess.encoding


def clean_prose(text):
    text = INVALID.sub("", text).replace("\r\n", "\n").replace("\r", "\n")
    # HTML code blocks can occur in existing Markdown and converted HTML.
    protected = []

    def protect(match):
        protected.append(match.group())
        return f"CLEAN_CODE_BLOCK_{len(protected) - 1}"

    text = re.sub(r"<(pre|code)\b[^>]*>.*?</\1>", protect, text, flags=re.S | re.I)
    lines = []
    fence = None
    for line in text.split("\n"):
        match = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
            lines.append(line)
            continue
        if fence or line.startswith(("    ", "\t")):
            lines.append(line)
            continue
        # Preserve indentation and intentional Markdown hard line breaks.
        indent = re.match(r"^[ \t]*", line).group()
        hard_break = "  " if line.endswith("  ") else ""
        body = re.sub(r"[ \t\u00a0]+", " ", line[len(indent):]).strip()
        normalized = indent + body + hard_break if body else ""
        if normalized or not lines or lines[-1]:
            lines.append(normalized)
    result = "\n".join(lines).strip("\n") + "\n"
    for index, block in enumerate(protected):
        result = result.replace(f"CLEAN_CODE_BLOCK_{index}", block)
    return result


def fenced(text, language):
    longest = max((len(m.group()) for m in re.finditer(r"`+", text)), default=0)
    marker = "`" * max(3, longest + 1)
    return f"{marker}{language}\n{text.rstrip(chr(10))}\n{marker}\n"


class HTMLMarkdown(HTMLParser):
    """Retain document blocks, lists, links, code and HTML tables in Markdown."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.suppressed = 0
        self.pre = False
        self.code = False
        self.lists = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in {"script", "style", "nav", "footer", "form"}:
            self.suppressed += 1
        if self.suppressed:
            return
        if re.fullmatch(r"h[1-6]", tag):
            self.parts.append("\n\n" + "#" * int(tag[1]) + " ")
        elif tag in {"p", "div", "section", "article", "main", "dl", "dt", "dd"}:
            self.parts.append("\n\n")
        elif tag in {"ul", "ol"}:
            self.lists.append(tag)
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("\n" + "  " * max(0, len(self.lists) - 1)
                              + ("1. " if self.lists and self.lists[-1] == "ol" else "- "))
        elif tag == "pre":
            self.pre = True
            self.parts.append("\n\n<pre>")
        elif tag == "code" and not self.pre:
            self.code = True
            self.parts.append("<code>")
        elif tag in {"table", "thead", "tbody", "tr", "th", "td"}:
            attributes = "".join(f' {k}="{html.escape(v, quote=True)}"'
                                 for k, v in attrs.items() if k in {"colspan", "rowspan"})
            self.parts.append(("\n\n" if tag == "table" else "") + f"<{tag}{attributes}>")
        elif tag in {"strong", "b"}:
            self.parts.append("**")
        elif tag in {"em", "i"}:
            self.parts.append("*")
        elif tag == "br":
            self.parts.append("\n")
        elif tag == "a":
            self.links.append(attrs.get("href", ""))
            self.parts.append("[")
        elif tag == "img":
            self.parts.append(f"![{attrs.get('alt', '')}](<{attrs.get('src', '')}>)")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "nav", "footer", "form"}:
            self.suppressed = max(0, self.suppressed - 1)
            return
        if self.suppressed:
            return
        if tag == "pre":
            self.parts.append("</pre>\n\n")
            self.pre = False
        elif tag == "code" and not self.pre:
            self.code = False
            self.parts.append("</code>")
        elif tag in {"table", "thead", "tbody", "tr", "th", "td"}:
            self.parts.append(f"</{tag}>" + ("\n\n" if tag == "table" else ""))
        elif tag in {"ul", "ol"}:
            if self.lists:
                self.lists.pop()
            self.parts.append("\n")
        elif tag in {"p", "div", "section", "article", "main"} or re.fullmatch(r"h[1-6]", tag):
            self.parts.append("\n\n")
        elif tag in {"strong", "b"}:
            self.parts.append("**")
        elif tag in {"em", "i"}:
            self.parts.append("*")
        elif tag == "a" and self.links:
            self.parts.append(f"](<{self.links.pop()}>)")

    def handle_data(self, data):
        if not self.suppressed:
            self.parts.append(html.escape(data) if self.pre or self.code else re.sub(r"\s+", " ", data))


def docx_markdown(path):
    with zipfile.ZipFile(path) as archive:
        document = ET.fromstring(archive.read("word/document.xml"))
        rels = ET.fromstring(archive.read("word/_rels/document.xml.rels"))
        targets = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
        styles = {}
        if "word/styles.xml" in archive.namelist():
            for style in ET.fromstring(archive.read("word/styles.xml")):
                name = style.find(W + "name")
                if name is not None:
                    styles[style.get(W + "styleId")] = name.get(W + "val", "")

        def inline(node):
            if node.tag == W + "t":
                return node.text or ""
            if node.tag in {W + "br", W + "cr"}:
                return "\n"
            if node.tag == W + "tab":
                return "\t"
            if node.tag == A + "blip":
                target = targets.get(node.get(R + "embed"))
                if target:
                    member = str(PurePosixPath("word") / target)
                    mime = IMAGES.get(PurePosixPath(target).suffix.lower(), "application/octet-stream")
                    data = base64.b64encode(archive.read(member)).decode("ascii")
                    return f"\n![{PurePosixPath(target).name}](data:{mime};base64,{data})\n"
            value = "".join(inline(child) for child in node)
            if node.tag == W + "p":
                return value + "\n"
            if node.tag == W + "hyperlink" and node.get(R + "id") in targets:
                return f"[{value}](<{targets[node.get(R + 'id')]}>)"
            return value

        parts = []
        for block in document.find(W + "body"):
            if block.tag == W + "p":
                text = inline(block)
                style = block.find(f"{W}pPr/{W}pStyle")
                name = styles.get(style.get(W + "val"), "") if style is not None else ""
                heading = re.search(r"heading\s*(\d)", name, re.I)
                if heading:
                    text = "#" * min(6, int(heading[1])) + " " + text
                elif name.lower() == "title":
                    text = "# " + text
                elif block.find(f"{W}pPr/{W}numPr") is not None:
                    text = "- " + text
                parts.append(text)
            elif block.tag == W + "tbl":
                rows = []
                for row in block.findall(W + "tr"):
                    cells = [html.escape(inline(cell)).replace("\n", "<br>")
                             for cell in row.findall(W + "tc")]
                    rows.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
                parts.append("<table>\n" + "\n".join(rows) + "\n</table>")
        return clean_prose("\n\n".join(parts))


def convert(path, destination, suffix):
    if suffix == ".docx":
        destination.write_text(docx_markdown(path), encoding="utf-8")
        return {"format": "docx"}
    if suffix in IMAGES:
        payload = base64.b64encode(path.read_bytes()).decode("ascii")
        destination.write_text(f"![{path.name}](data:{IMAGES[suffix]};base64,{payload})\n", encoding="utf-8")
        return {"format": "image", "note": "Original image embedded; no OCR performed"}
    encoding = encoding_for(path)
    metadata = {"encoding": encoding}
    with path.open(encoding=encoding, newline="") as source, destination.open("w", encoding="utf-8", newline="\n") as output:
        if suffix in {".csv", ".tsv"}:
            csv.field_size_limit(2**31 - 1)
            # CSV inside a code fence is Markdown and preserves column names,
            # empty cells, multiline values and all records without table escaping.
            marker = "~" * 64
            output.write(marker + "csv\n")
            writer = csv.writer(output, delimiter="\t" if suffix == ".tsv" else ",", lineterminator="\n")
            count = 0
            removed = 0
            for row in csv.reader(source, delimiter="\t" if suffix == ".tsv" else ",", strict=True):
                cleaned = []
                for cell in row:
                    value, n = INVALID.subn("", cell)
                    if marker in value:
                        raise ValueError("CSV contains the reserved Markdown fence")
                    cleaned.append(value)
                    removed += n
                writer.writerow(cleaned)
                count += 1
            output.write(marker + "\n")
            metadata.update(rows=count, removed_invalid_characters=removed)
        elif suffix == ".jsonl":
            count = 0
            for number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    json.loads(line)
                except ValueError as exc:
                    raise ValueError(f"Invalid JSONL record on line {number}: {exc}") from exc
                output.write(f"## Record {count + 1}\n\n" + fenced(line.strip(), "json") + "\n")
                count += 1
            metadata["records"] = count
        elif suffix == ".json":
            text = source.read()
            json.loads(text)
            # Retain original numeric lexemes, duplicate keys, escaped control
            # characters and string whitespace; never round-trip through floats.
            output.write(fenced(text.strip(), "json"))
        elif suffix == ".html":
            parser = HTMLMarkdown()
            parser.feed(source.read())
            parser.close()
            output.write(clean_prose("".join(parser.parts).strip()))
        elif suffix in TEXT_TYPES:
            if suffix in {".yaml", ".yml", ".sql", ".log", ".md5"}:
                text = INVALID.sub("", source.read()).replace("\r\n", "\n")
                output.write(fenced(text, {".yml": "yaml", ".log": "text", ".md5": "text"}.get(suffix, suffix[1:])))
            else:
                output.write(clean_prose(source.read()))
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent
    parser.add_argument("--source", type=Path, default=base / "data")
    parser.add_argument("--output", type=Path, default=base / "data_clean")
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    if not source.is_dir() or source == output or source in output.parents or output in source.parents:
        parser.error("Source must exist and output must be a separate, non-overlapping directory")
    if os.name == "nt":
        # Preserve deep archive paths even when Windows long paths are disabled.
        output = Path("\\\\?\\UNC\\" + str(output)[2:] if str(output).startswith("\\\\")
                      else "\\\\?\\" + str(output))
    output.mkdir(parents=True, exist_ok=True)
    entries = []

    def process(path, relative, archived=False):
        entry = {"source": relative.as_posix(), "archived": archived}
        entries.append(entry)
        suffix = relative.suffix.lower()
        if "来源说明" in relative.parts:
            entry.update(status="skipped", reason="按要求排除来源说明目录")
            return
        if relative.name == ".DS_Store" or "__MACOSX" in relative.parts or suffix == ".pkl":
            entry.update(status="skipped", reason="System metadata" if suffix != ".pkl" else "Pickle cache; not deserialized (CSV/YAML counterparts processed)")
            return
        destination = output / relative
        if suffix != ".md":
            destination = destination.with_name(destination.name + ".md")
        temporary = destination.with_name(destination.name + ".tmp")
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if suffix == ".zip":
                with zipfile.ZipFile(path) as archive, tempfile.TemporaryDirectory(prefix=".clean-work-", dir=output) as work:
                    member_entries = []
                    for info in archive.infolist():
                        if info.is_dir():
                            continue
                        name = PurePosixPath(info.filename.replace("\\", "/"))
                        if name.is_absolute() or ".." in name.parts or any(":" in p for p in name.parts):
                            entries.append({"source": f"{relative}/{info.filename}", "archived": True, "status": "failed", "error": "Unsafe archive member path"})
                            continue
                        member_relative = relative / Path(*name.parts)
                        # Each member is streamed to a fixed temporary path;
                        # archive-provided paths are never used for extraction.
                        member = Path(work) / ("member" + name.suffix)
                        try:
                            with archive.open(info) as incoming, member.open("wb") as outgoing:
                                while chunk := incoming.read(1048576):
                                    outgoing.write(chunk)
                            process(member, member_relative, archived=True)
                        except Exception as exc:
                            entries.append({"source": member_relative.as_posix(), "archived": True, "status": "failed", "error": str(exc)})
                        member_entries.append(entries[-1])
                    manifest = "# " + relative.name + "\n\n" + "\n".join(
                        f"- {e['source']}: {e['status']}" for e in member_entries) + "\n"
                    temporary.write_text(manifest, encoding="utf-8")
                    entry["members"] = len(member_entries)
            else:
                entry.update(convert(path, temporary, suffix))
            temporary.replace(destination)
            entry.update(status="success", output=destination.relative_to(output).as_posix())
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            entry.update(status="failed", error=f"{type(exc).__name__}: {exc}")

    originals = sorted(p for p in source.rglob("*") if p.is_file())
    hashes = {}
    for number, path in enumerate(originals, 1):
        try:
            with path.open("rb") as stream:
                hashes[path.relative_to(source).as_posix()] = hashlib.file_digest(stream, "sha256").hexdigest()
        except OSError as exc:
            entries.append({"source": path.relative_to(source).as_posix(), "archived": False,
                            "status": "failed", "error": str(exc)})
            continue
        process(path, path.relative_to(source))
        print(f"[{number}/{len(originals)}] {path.name}", flush=True)
    summary = {"source_files": dict(Counter(e["status"] for e in entries if not e["archived"])),
               "archive_members": dict(Counter(e["status"] for e in entries if e["archived"])),
               "total": dict(Counter(e["status"] for e in entries))}
    for counts in summary.values():
        for status in ("success", "failed", "skipped"):
            counts.setdefault(status, 0)
    report = {"summary": summary, "source_sha256": hashes, "files": entries,
              "notes": ["Source filename retained with .md appended; existing .md names unchanged.",
                        "JSON/JSONL field values and numeric representations are preserved verbatim.",
                        "Images embedded as original bytes, without OCR; Pickle caches skipped.",
                        "ZIP containers and their members counted separately."]}
    (output / "_cleaning_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
