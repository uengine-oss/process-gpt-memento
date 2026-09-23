# -*- coding: utf-8 -*-
"""XLSX 첫 시트는 1쪽이어야 한다.

회귀: 시트 번호를 1-based 로 넣고 저장 단계가 0-based 로 보고 +1 해 첫 시트가 2쪽이 됐다.
"""
import io

from openpyxl import Workbook

from app.services.document_pages import _extract_page_number
from app.services.document_processor import _load_xlsx_documents


def test_first_sheet_is_page_one():
    wb = Workbook()
    wb.active.title = "2025년"
    wb.active.append(["사업명", "금액"])
    wb.create_sheet("2026년").append(["사업명", "금액"])
    buffer = io.BytesIO()
    wb.save(buffer)

    docs = _load_xlsx_documents(buffer.getvalue(), "book.xlsx")

    assert [_extract_page_number(doc, i) for i, doc in enumerate(docs)] == [1, 2]
