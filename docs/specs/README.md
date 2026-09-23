# specs

이 저장소가 지키기로 한 계약. **현재 코드가 실제로 하는 일**을 적는다.
바라는 바나 계획이 아니다.

| Spec | 다루는 것 |
|---|---|
| [`knowledge-map.md`](knowledge-map.md) | 인제스트 → 페이지 → 문서·폴더 카드 → 지도 API, 상태, 호출처 |
| [`artifact-bucket.md`](artifact-bucket.md) | 에이전트 산출물의 비공개 보관과 서명 주소 |

## 쓰는 법

- 계약을 바꾸는 변경이라면 **코드보다 spec을 먼저 읽는다.**
- 코드를 바꿨으면 spec도 같이 고친다.
- 규칙에 이유가 있으면 [`../DESIGN_NOTES.md`](../DESIGN_NOTES.md)에 근거를 남긴다 —
  근거가 없으면 다음 사람이 "이거 왜 이렇게 했지" 하며 되돌린다.
