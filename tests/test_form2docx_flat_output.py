# -*- coding: utf-8 -*-
"""form_to_docx 가 평면 output 에서도 죽지 않아야 한다.

회귀: 워크아이템 output 은 {form_id: {...}} 만이 아니라 평면 키가 먼저 오는 경우가
흔한데(사람 제출·에이전트 산출 모두), 예전 구현은 '첫 키' 를 폼 루트로 가정해
'str' object has no attribute 'get' 로 500 을 냈고 구글 드라이브 적재가 전량 실패했다.
"""
import pytest

from app.converters.form2docx import form_to_docx, resolve_form_values

FORM_ID = "sales_process_task1_form"
HTML = (
    '<section><row-layout name="customer_info" alias="고객 정보" is_multidata_mode="false" '
    'v-model="formValues" v-slot="slotProps">'
    '<div class="row"><div class="col-sm-6">'
    '<text-field name="customer_company" alias="고객사명" type="text" '
    "v-model=\"slotProps.modelValue['customer_company']\"></text-field>"
    '</div><div class="col-sm-6">'
    '<select-field name="review_required" alias="검토 필요 여부" '
    "items=\"[{'예':'검토 필요'},{'아니오':'검토 불필요'}]\" "
    "v-model=\"slotProps.modelValue['review_required']\"></select-field>"
    "</div></div></row-layout></section>"
)

FIELDS = {"customer_company": "유엔진테스트(주)", "review_required": "예"}


def test_nested_output():
    assert resolve_form_values({FORM_ID: FIELDS}, FORM_ID) == FIELDS
    assert form_to_docx(HTML, {FORM_ID: FIELDS}, FORM_ID)


def test_flat_keys_first_does_not_crash():
    """평면 키가 앞에 오고 form_id 가 뒤에 오는, 실제 엔진이 넘기는 모양."""
    output = dict(FIELDS)
    output[FORM_ID] = FIELDS
    assert resolve_form_values(output, FORM_ID) == FIELDS
    assert form_to_docx(HTML, output, FORM_ID)


def test_flat_only_output():
    """에이전트 산출물처럼 form_id 중첩이 아예 없는 경우."""
    assert resolve_form_values(FIELDS, FORM_ID) == FIELDS
    assert form_to_docx(HTML, FIELDS, FORM_ID)


def test_form_id_unknown_falls_back_to_form_suffix_key():
    output = {"text": "에이전트 원문", FORM_ID: FIELDS}
    assert resolve_form_values(output, None) == FIELDS


def test_empty_output():
    assert resolve_form_values({}, FORM_ID) == {}
    assert form_to_docx(HTML, {}, FORM_ID)


@pytest.mark.parametrize("bad", [None, "문자열", 3])
def test_non_dict_output(bad):
    assert resolve_form_values(bad, FORM_ID) == {}
