"""가로선 없이 한 행에 묶인 하위 행을 글자 줄로 나눈다(find_tables 결과 후처리).

한컴 통계표는 하위 행 사이에 선이 없어 find_tables 가 수십 행을 한 칸에 담는다. 값은 남지만 행 머리와의 짝이
사라져 LLM 이 몇 번째 줄인지 세다 틀린다. 근거·측정은 docs/DESIGN_NOTES.md "선 없는 하위 행".
"""
from __future__ import annotations

import re
from typing import List, Optional

_NUM = re.compile(r"^[-−–+△▲▽▼↑↓]?\(?[-−–+△▲▽▼↑↓]?[\d][\d.,]*%?\)?[a-zA-Z*]{0,2}$")
_BAND = 0.5  # 줄 높이의 이 비율보다 가까운 글자는 같은 줄
_PROSE = 40  # 칸 한 줄이 이보다 길면 문단


def _is_num(s: str) -> bool:
    # "220 (31.3)" 처럼 비율을 붙인 값도 값이다
    return bool(_NUM.match(s.strip().split(" (")[0]))


def _md_rows(md: str) -> List[List[str]]:
    rows = []
    for ln in md.strip().splitlines():
        body = ln.strip()
        if not body.startswith("|"):
            continue
        cells = [c.strip() for c in body.strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):
            continue
        rows.append(cells)
    return rows


def _to_md(rows: List[List[str]]) -> str:
    n = max(len(r) for r in rows)
    rows = [r + [""] * (n - len(r)) for r in rows]
    out = ["|" + "|".join(rows[0]) + "|", "|" + "---|" * n]
    out += ["|" + "|".join(r) + "|" for r in rows[1:]]
    return "\n".join(out) + "\n"


def _col_edges(tab) -> List[float]:
    xs = sorted({round(c[0], 1) for row in tab.rows for c in row.cells if c})
    return xs + [tab.bbox[2]]


def _chars(page, clip) -> List[tuple]:
    """(x0, y0, x1, y1, 글자). 이름과 값 사이에 공백 문자가 없는 PDF 가 있어 단어가 아니라 글자로 칸을 나눈다."""
    out = []
    for block in page.get_text("rawdict", clip=clip).get("blocks", []):
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                for ch in span.get("chars", []):
                    if ch["c"].strip():
                        out.append((*ch["bbox"], ch["c"]))
    return out


def _bands(chars) -> List[list]:
    """같은 줄(세로 중심이 가까운) 글자 묶음, 위에서 아래로."""
    chars = sorted(chars, key=lambda w: (w[1] + w[3]) / 2)
    bands: List[list] = []  # [중심, 높이, 글자들]
    for w in chars:
        mid, h = (w[1] + w[3]) / 2, w[3] - w[1]
        # 위첨자(2024ᵖ)는 중심이 조금 떠 있어 줄 높이는 그 줄에서 가장 큰 글자로 본다
        if bands and abs(mid - bands[-1][0]) <= _BAND * max(h, bands[-1][1], 1.0):
            bands[-1][2].append(w)
            bands[-1][1] = max(bands[-1][1], h)
        else:
            bands.append([mid, h, [w]])
    return [b[2] for b in bands]


def _tokens(chars, edges) -> List[tuple]:
    """한 줄 글자를 보이는 간격(글자 높이의 1/4 초과)으로 끊은 (x0, x1, 글).
    칸 경계를 넘을 때는 조금만 벌어져도 끊는다(좁은 칸에 오른쪽 정렬한 이웃 숫자). 붙어서 넘친 글은 한 덩어리."""
    out: List[list] = []
    for w in sorted(chars, key=lambda w: w[0]):
        gap = w[0] - out[-1][1] if out else 0
        crosses = out and any(out[-1][1] - 0.5 <= e <= w[0] + 0.5 for e in edges[1:-1])
        if out and gap <= 0.25 * (w[3] - w[1]) and not (crosses and gap > 0.5):
            out[-1][1] = max(out[-1][1], w[2])
            out[-1][2] += w[4]
        else:
            out.append([w[0], w[2], w[4]])
    return [tuple(t) for t in out]


def _chars_by_row(rows, chars) -> List[list]:
    """글자마다 그 글자를 감싸는 가장 작은 칸의 행에 배정한다. 병합 칸·자잘한 행이 겹쳐도 한 글자는 한 행에만."""
    cells = [(r, c) for r, row in enumerate(rows) for c in row.cells if c]
    out: List[list] = [[] for _ in rows]
    for w in chars:
        xc, yc = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
        best = None
        for r, c in cells:
            if c[0] <= xc <= c[2] and c[1] <= yc <= c[3]:
                area = (c[2] - c[0]) * (c[3] - c[1])
                if best is None or area < best[0]:
                    best = (area, r)
        if best:
            out[best[1]].append(w)
    return out


def _runs(idx: List[int]) -> List[List[int]]:
    """연달아 붙은 줄 번호끼리 묶는다."""
    out: List[List[int]] = []
    for i in idx:
        if out and out[-1][-1] == i - 1:
            out[-1].append(i)
        else:
            out.append([i])
    return out


def _nearest(i: int, runs: List[List[int]], mids: List[float]) -> List[int]:
    """줄 i 에 세로로 가장 가까운 묶음. 두 값 줄 가운데 찍힌 이름은 반 줄, 위 행 이름은 한 줄 떨어져 있다.
    거리가 같으면 위 묶음."""
    return min(runs, key=lambda run: (round(min(abs(mids[i] - mids[j]) for j in run)), run[0] > i))


def _split_row(inside, edges, n_cols) -> Optional[List[List[str]]]:
    bands = _bands(inside)
    if len(bands) < 2:
        return None
    grid = []
    for band in bands:
        cells = [[] for _ in range(n_cols)]
        for t in _tokens(band, edges):
            xc = (t[0] + t[1]) / 2
            cells[max(0, min(n_cols - 1, sum(1 for e in edges[1:-1] if xc >= e)))].append(t[2])
        grid.append([" ".join(c) for c in cells])
    # 각주 표시만 든 칸(위첨자 *)은 따로 행이 아니다. 위첨자는 제 줄보다 떠 있어 아래 줄 것이다. "-"(해당 없음)는 값이다
    for r in range(len(grid) - 1):
        for c, text in enumerate(grid[r]):
            if text and set(text) <= set("*†‡") and any(grid[r + 1]):
                grid[r + 1][c] += text
                grid[r][c] = ""
    # 값 아래 (비율)·(0.8배)를 쌓은 칸: 괄호는 위 줄 값 밑에만, 다른 글(두 줄 가운데 찍힌 4세)은 위 줄 빈칸에만 있으면
    # 위 줄의 부속이다 — "값 (비율)" 로 붙인다. 위 줄 이름 밑에 제 이름이 있으면(총투자/증감률) 제 행
    merged: List[List[str]] = []
    mids: List[float] = []  # 줄의 세로 위치 — 이름을 가까운 값 줄에 붙일 때 쓴다
    for g, band in zip(grid, bands):
        if not any(g):
            continue
        paren = [bool(c) and c.startswith("(") and c.endswith(")") for c in g]
        fits = (merged and sum(_is_num(c) for c in merged[-1] if c) >= 2
                and all((bool(a) if p else not a) for a, b, p in zip(merged[-1], g, paren) if b))
        # 두 줄 가운데 찍힌 값(주민등록 유아 수 70,251)도 위 줄 빈칸에만 있으면 같은 행이다
        if fits and (any(paren) or any(_is_num(c) for c in g if c)):
            merged[-1] = [f"{a} {b}".strip() for a, b in zip(merged[-1], g)]
        else:
            merged.append(g)
            mids.append(sum((w[1] + w[3]) / 2 for w in band) / len(band))
    grid = merged
    # 값 줄이 둘 이상이어야 하위 행이다. 머리 칸의 줄바꿈(확정치/(A))은 그대로 둔다
    is_val = [sum(_is_num(c) for c in g if c) >= 2 for g in grid]
    vrows = [g for g, v in zip(grid, is_val) if v]
    if len(vrows) < 2:
        return None
    # 값 열은 값 줄마다 숫자가 차 있는 열. 그 앞(연도처럼 묶음 첫 줄에만 적힌 이름)은 위 값을 잇는다
    # 이름 열: 값 줄 절반 이상에 숫자 아닌 이름(1/4, 전월비)이 있는 열과 그 왼쪽(연도). 없으면 숫자가 드문 앞 열
    named = [c for c in range(n_cols) if sum(bool(g[c]) and not _is_num(g[c]) for g in vrows) >= 0.5 * len(vrows)]
    first_num = max(named) + 1 if named else next(
        (c for c in range(n_cols) if sum(_is_num(g[c]) for g in vrows) >= 0.5 * len(vrows)), n_cols)
    for c in range(first_num):
        parts = [g[c] for g in grid if g[c]]
        # 세로쓰기 이름(생/산)은 한 글자씩 줄에 걸쳐 있다 — 합쳐서 모든 하위 행에
        if len(parts) >= 2 and all(len(p) == 1 for p in parts):
            for g in grid:
                g[c] = "".join(parts)
    # 값 없이 이름만 있는 줄: 여러 줄 이름(담배제품/현재/사용률), 두 값 줄 사이 가운데 이름(백화점),
    # 묶음 이름(서비스업). 이어진 이름 줄을 한 이름으로 묶고, 값 줄마다 가장 가까운 이름을 붙인다
    lab_idx = [i for i, (g, v) in enumerate(zip(grid, is_val)) if not v and not any(g[first_num:])]
    val_idx = [i for i, v in enumerate(is_val) if v]
    # 이름 줄이 값 줄만큼 있으면 하위 행이 아니라 줄바꿈된 이름 칸 옆에 값/(비중)을 쌓은 칸이다
    if len(lab_idx) >= len(val_idx):
        return None
    if lab_idx:
        for c in range(first_num):
            texts = [g[c] for g in grid]
            if not any(texts[i] for i in lab_idx):
                continue
            if all(texts[i] for i in val_idx):  # 값 줄마다 제 이름이 있으면 바로 옆 묶음 이름(항공 → 국내·국제)만 앞에
                runs = _runs([i for i in lab_idx if texts[i]])
                for i in val_idx:
                    run = _nearest(i, runs, mids)
                    if min(abs(i - j) for j in run) == 1:
                        grid[i][c] = f"{' '.join(texts[j] for j in run)} {texts[i]}"
            else:
                runs = _runs([i for i, t in enumerate(texts) if t])
                for i in val_idx:
                    grid[i][c] = " ".join(texts[j] for j in _nearest(i, runs, mids))
        grid = [g for i, g in enumerate(grid) if i not in lab_idx or any(g[first_num:])]
    for r in range(1, len(grid)):
        for c in range(first_num):
            if not grid[r][c] and any(grid[r][first_num:]):
                grid[r][c] = grid[r - 1][c]
    return grid if _regular(grid, first_num) else None


_GROUPED = re.compile(r"^[-−–+△▲▽▼↑↓(]*\d{1,3}(,\d{3})+(\.\d+)?[)%*a-zA-Z]*$")


def _regular(grid: List[List[str]], first_num: int) -> bool:
    """나눈 결과가 반듯한 격자일 때만 쓴다. 아니면 원래 칸(<br> 묶음)이 낫다.
    값 줄마다 값 칸 수가 비슷하고, 한 칸에 숫자 하나("값 (비율)" 은 허용), 쉼표 묶음이 세 자리여야 한다."""
    vals = [g[first_num:] for g in grid if sum(_is_num(c) for c in g if c) >= 2]
    filled = [sum(bool(c) for c in v) for v in vals]
    if not vals or min(filled) < 0.6 * max(filled):
        return False
    for v in vals:
        for cell in v:
            head = cell.split(" (")[0]
            # "2024. 8"(연. 월)은 값 하나다
            if sum(_is_num(t) and not t.endswith(".") for t in head.split()) > 1:
                return False
            if "," in head and _is_num(head) and not _GROUPED.match(head):
                return False
    return True


def split_subrows(page, tab, md: str) -> str:
    """md 는 tab.to_markdown() 결과. 하위 행을 나눈 표 마크다운(나눌 것이 없으면 그대로)."""
    try:
        md_rows = _md_rows(md)
        rows = list(tab.rows)
        offset = 0 if tab.header.external else 1  # md 첫 줄이 표 밖 머리면 데이터 행이 rows[0]부터
        if not md_rows or len(md_rows) - 1 + offset != len(rows):
            return md
        edges = _col_edges(tab)
        n_cols = len(md_rows[0])
        if len(edges) - 1 != n_cols:
            return md
        by_row = _chars_by_row(rows, _chars(page, tab.bbox))
        out, changed, kept = [md_rows[0]], False, []
        for i, cells in enumerate(md_rows[1:]):
            # 문단이 든 행(쪽 테두리를 표로 잡은 경우)은 나누면 낱말이 칸으로 흩어진다
            prose = any(len(seg) > _PROSE for c in cells for seg in c.split("<br>"))
            sub = None if prose else _split_row(by_row[i + offset], edges, n_cols)
            if not sub:
                # 제 글자가 없는 행은 병합 칸 복사본뿐이다(find_tables 가 만든 자잘한 행)
                if any(w[4].isalnum() for w in by_row[i + offset]):
                    kept.append(len(out))
                out.append(cells)
                continue
            changed = True
            # 위 행에서 내려온 병합 칸(행 안에 글자가 없는 칸)은 원래 채운 값을 쓴다
            for c in [c for c in range(n_cols) if cells[c] and not any(s[c] for s in sub)]:
                for s in sub:
                    # 가로 병합으로 옆 칸 값이 복사된 칸은 하위 행에서도 옆 칸을 따른다
                    if c + 1 < n_cols and cells[c] == cells[c + 1]:
                        s[c] = s[c + 1]
                    elif c > 0 and cells[c] == cells[c - 1]:
                        s[c] = s[c - 1]
                    else:
                        parts = cells[c].split("<br>")
                        s[c] = ("" if all(len(p) == 1 for p in parts) else " ").join(parts)
            kept.extend(range(len(out), len(out) + len(sub)))
            out.extend(sub)
        if not changed:
            return md
        return _to_md([out[0]] + [out[k] for k in kept if k > 0])
    except Exception:
        return md
