# -*- coding: utf-8 -*-
"""register_uploaded_file 호출은 실제 시그니처에 있는 키워드만 넘겨야 한다.

회귀: doc_role 인자를 없앤 변경과 그 인자를 넘기는 산출물 저장 코드가 충돌 없이 병합돼
/save-to-storage 비공개 갈래가 TypeError(500)로 죽었다.
"""
import ast
import inspect
from pathlib import Path

from app.services.knowledge_files import register_uploaded_file

APP = Path(__file__).resolve().parents[1] / "app"


def test_register_uploaded_file_calls_use_known_keywords():
    allowed = set(inspect.signature(register_uploaded_file).parameters)
    bad = []
    for path in APP.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", "")) == "register_uploaded_file":
                bad += [f"{path.relative_to(APP)}:{node.lineno} {kw.arg}" for kw in node.keywords
                        if kw.arg and kw.arg not in allowed]
    assert not bad, bad
