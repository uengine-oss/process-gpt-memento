"""카드·요약 프롬프트가 영어 문서에는 영어로 쓰라고 직접 말하는지."""
from app.services.doc_cards import Window, build_first_prompt
from app.services.summary_prompts import build_doc_card_abstract_prompt, build_mini_summary_prompt
from app.services.text_language import english_output_rule, text_language

EN = "Hanbit Office Price List and Discount Rules (2026). Mesh Chair Pro HB-C02 KRW 260,000. Volume discount 20-49 units 5%."
KO = "한빛오피스 제품 가격표와 할인 규정(2026). 메쉬 의자 프로 HB-C02 260,000원. 수량 할인 20~49개 5%."


def test_text_language():
    assert text_language(EN) == "en"
    assert text_language(KO) == "ko"
    assert text_language("") == "ko"
    assert english_output_rule(KO, "summary") == ""


def test_card_prompt_names_english():
    en = build_first_prompt("Hanbit-Office-Price-List.pdf", Window(index=0, total=1, start=0, end=len(EN), text=EN))
    ko = build_first_prompt("한빛오피스-제품가격표.pdf", Window(index=0, total=1, start=0, end=len(KO), text=KO))
    assert "이 자료는 영어다" in en
    assert "이 자료는 영어다" not in ko


def test_summary_prompts_follow_language():
    assert "영어 평문" in build_doc_card_abstract_prompt(EN)
    assert "한국어 평문" in build_doc_card_abstract_prompt(KO)
    assert "영어 불릿" in build_mini_summary_prompt("", 1, 1, 1, 1, EN)
    assert "한국어 불릿" in build_mini_summary_prompt("", 1, 1, 1, 1, KO)
