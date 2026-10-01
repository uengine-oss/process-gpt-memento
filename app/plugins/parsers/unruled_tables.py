"""괘선 없는 표를 PDF 글 블록의 배치에서 되살린다. 근거: docs/DESIGN_NOTES.md#괘선-없는-표

표의 한 행은 같은 높이에 글이 옆으로 늘어선다(블록 하나 안의 줄들, 또는 이어지는 블록들). 본문 문단은 줄이
위아래로 쌓인다. 옆으로 늘어선 행이 세 개 이상 이어지고 칸 수와 칸의 가로 범위가 맞으면 표로 본다.
글을 지우거나 순서를 바꾸지 않고 같은 줄을 표로 감싸기만 한다. 칸 안에서 줄이 바뀌는 행은 표로 보지 않는다.
"""
from __future__ import annotations

from collections import Counter
from statistics import median
from typing import Any, Dict, List, Optional

MIN_ROWS = 3
MIN_CELLS = 2
# 한 행으로 볼 세로 어긋남(줄 높이 대비), 행 사이 빈틈 상한(줄 높이 대비)
_BAND = 0.5
_ROW_GAP = 2.0
# 칸 사이 최소 틈(줄 높이 대비). 한 줄이 조각나 추출된 본문·목록·목차는 틈이 이보다 좁았다(0.66~0.87), 표는 1.17 이상.
_CELL_GAP = 1.0

Cell = Dict[str, Any]  # {"bbox": [x0, y0, x1, y1], "text": str}


def line_cells(block: Optional[dict]) -> Optional[List[Cell]]:
    """PyMuPDF dict 블록의 줄들이 한 높이에 옆으로 늘어서 있으면 왼쪽부터 칸 목록, 아니면 None."""
    if not block:
        return None
    cells = []
    for line in block.get("lines", []):
        text = "".join(s.get("text", "") for s in line.get("spans", [])).strip()
        if text:
            cells.append({"bbox": list(line["bbox"]), "text": text})
    if not cells:
        return None
    height = median(c["bbox"][3] - c["bbox"][1] for c in cells) or 1.0
    centers = [(c["bbox"][1] + c["bbox"][3]) / 2 for c in cells]
    if max(centers) - min(centers) > height * _BAND:
        return None  # 줄이 위아래로 쌓였다 — 문단
    cells.sort(key=lambda c: c["bbox"][0])
    if any(b["bbox"][0] - a["bbox"][2] < height * _CELL_GAP for a, b in zip(cells, cells[1:])):
        return None  # 칸 사이가 좁다 — 한 줄이 조각난 것
    return cells


def _row_of(cells: List[Cell]) -> Dict[str, Any]:
    top = min(c["bbox"][1] for c in cells)
    bottom = max(c["bbox"][3] for c in cells)
    return {"cells": cells, "top": top, "bottom": bottom, "center": (top + bottom) / 2,
            "height": median(c["bbox"][3] - c["bbox"][1] for c in cells) or 1.0}


def _same_row(row: Dict[str, Any], cells: List[Cell]) -> bool:
    nxt = _row_of(cells)
    return abs(nxt["center"] - row["center"]) <= row["height"] * _BAND and \
        cells[0]["bbox"][0] - row["cells"][-1]["bbox"][2] >= row["height"] * _CELL_GAP


def _continues(run: List[Dict[str, Any]], row: Dict[str, Any], columns: List[List[float]]) -> bool:
    last = run[-1]
    if len(row["cells"]) != len(last["cells"]):
        return False
    if not (last["bottom"] - 1 <= row["top"] <= last["bottom"] + last["height"] * _ROW_GAP):
        return False
    slack = last["height"] * _BAND
    return all(c["bbox"][0] <= col[1] + slack and c["bbox"][2] >= col[0] - slack
               for c, col in zip(row["cells"], columns))


def _markdown(run: List[Dict[str, Any]]) -> str:
    def line(row):
        return "| " + " | ".join(c["text"].replace("|", "\\|") for c in row["cells"]) + " |"
    head = line(run[0])
    sep = "| " + " | ".join("---" for _ in run[0]["cells"]) + " |"
    return "\n".join([head, sep] + [line(r) for r in run[1:]])


def _chars(text: str) -> Counter:
    return Counter(c for c in text if c.isalnum())


def rebuild(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """entries: 글 순서의 [{"key", "text", "bbox", "cells"(line_cells 결과 또는 None)}].

    표로 볼 행 묶음을 표 항목 하나로 바꾼 목록을 돌려준다. 나머지 항목은 그대로다.
    """
    units: List[Dict[str, Any]] = []  # {"idx": [...], "row": 행 또는 None}
    for i, e in enumerate(entries):
        cells = e.get("cells")
        last = units[-1] if units else None
        if cells and last and last["row"] and _same_row(last["row"], cells):
            last["idx"].append(i)
            last["row"] = _row_of(last["row"]["cells"] + cells)
        else:
            units.append({"idx": [i], "row": _row_of(cells) if cells else None})

    out: List[Dict[str, Any]] = []
    u = 0
    while u < len(units):
        row = units[u]["row"]
        run = [row] if row and len(row["cells"]) >= MIN_CELLS else []
        columns = [[c["bbox"][0], c["bbox"][2]] for c in row["cells"]] if run else []
        v = u + 1
        while run and v < len(units) and units[v]["row"] and _continues(run, units[v]["row"], columns):
            nxt = units[v]["row"]
            columns = [[min(col[0], c["bbox"][0]), max(col[1], c["bbox"][2])] for col, c in zip(columns, nxt["cells"])]
            run.append(nxt)
            v += 1
        if len(run) >= MIN_ROWS:
            first = u
            # 바로 위의 같은 칸 수 행은 머리행이다. 머리 글은 가운데·숫자는 오른쪽 정렬이라 칸 위치로는 안 맞는다.
            prev = units[u - 1] if u else None
            if prev and prev["row"] and len(prev["row"]["cells"]) == len(run[0]["cells"]) \
                    and out[-len(prev["idx"]):] == [entries[i] for i in prev["idx"]] \
                    and 0 <= run[0]["top"] - prev["row"]["bottom"] + 1 <= prev["row"]["height"] * _ROW_GAP + 1:
                del out[-len(prev["idx"]):]
                run.insert(0, prev["row"])
                first = u - 1
            idx = [i for w in units[first:v] for i in w["idx"]]
            md = _markdown(run)
            if _chars(md) != _chars(" ".join(entries[i]["text"] for i in idx)):
                # 표가 원래 블록의 글을 그대로 담지 못하면 바꾸지 않는다(글을 잃는 것보다 표를 놓치는 게 낫다)
                out.extend(entries[i] for i in idx)
                u = v
                continue
            boxes = [entries[i]["bbox"] for i in idx]
            out.append({"key": entries[idx[0]]["key"], "text": md, "cells": None,
                        "bbox": [min(b[0] for b in boxes), min(b[1] for b in boxes),
                                 max(b[2] for b in boxes), max(b[3] for b in boxes)]})
            u = v
        else:
            out.extend(entries[i] for i in units[u]["idx"])
            u += 1
    return out
