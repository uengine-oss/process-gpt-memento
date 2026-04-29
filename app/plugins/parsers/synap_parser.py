"""Synap DocuAnalyzer 원격 파서.

외부 회사 내부망에서 제공하는 Synap DocuAnalyzer(DA) HTTP API를 호출해
문서를 파싱한다. hwp/hwpx/pdf/docx/pptx/xlsx 등 DA 엔진이 수용하는
포맷이라면 로컬 라이브러리 없이 Markdown을 얻을 수 있다.

엔드포인트 흐름:
  1) POST /da               : 파일 업로드 → fid
  2) POST /filestatus/{fid} : 상태 폴링 (SUCCESS까지)
  3) POST /result/{fid}     : 페이지별 결과 조회
  4) POST /delete/{fid}     : 임시 파일 정리 (반드시 호출)

서버는 업로드 파일명을 항상 `upload.hwp`, MIME은
`application/octet-stream`으로 받는 것이 관례이므로 그대로 따른다.
"""
from __future__ import annotations

import asyncio
import time
from typing import List, Optional

import requests
from langchain.schema import Document

from . import config
from .base import BaseParser


class SynapParseError(RuntimeError):
    pass


class SynapParser(BaseParser):
    name = "synap"
    supported_extensions = (".hwp", ".hwpx", ".pdf", ".docx", ".pptx", ".xlsx")

    def __init__(self):
        self.api_key = config.SYNAP_API_KEY
        self.engine = (config.SYNAP_URL or "").rstrip("/")
        self.output_type = config.SYNAP_OUTPUT_TYPE
        self.poll_interval = config.SYNAP_POLL_INTERVAL
        self.poll_timeout = config.SYNAP_POLL_TIMEOUT
        self.request_timeout = config.SYNAP_REQUEST_TIMEOUT

        if not self.api_key or not self.engine:
            raise SynapParseError(
                "SYNAP_API_KEY / SYNAP_URL 환경변수가 설정되지 않았습니다."
            )

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        docs = await asyncio.to_thread(self._parse_sync, file_content, file_name)
        return self._tag(docs)

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        fid = self._upload(file_content)
        try:
            total_pages = self._wait_until_ready(fid)
            pages = self._fetch_pages(fid, total_pages)
        finally:
            self._delete(fid)

        docs: List[Document] = []
        for i, text in enumerate(pages):
            docs.append(Document(
                page_content=text or "",
                metadata={
                    "source": file_name,
                    "page": i,
                    "synap_fid": fid,
                    "synap_output": self.output_type,
                },
            ))
        return docs

    # ---- HTTP helpers ------------------------------------------------------

    def _upload(self, file_content: bytes) -> str:
        url = f"{self.engine}/da"
        data = {"api_key": self.api_key, "type": "upload"}
        files = {
            "file": ("upload.hwp", file_content, "application/octet-stream"),
        }
        resp = requests.post(url, data=data, files=files, timeout=self.request_timeout)
        payload = self._json_or_raise(resp, "upload")
        try:
            return payload["result"]["fid"]
        except (KeyError, TypeError) as e:
            raise SynapParseError(f"Synap 업로드 응답에 fid가 없음: {payload}") from e

    def _wait_until_ready(self, fid: str) -> int:
        url = f"{self.engine}/filestatus/{fid}"
        body = {"api_key": self.api_key}
        deadline = time.monotonic() + self.poll_timeout
        last_status = None
        while True:
            resp = requests.post(url, json=body, timeout=self.request_timeout)
            payload = self._json_or_raise(resp, "filestatus")
            result = payload.get("result") or {}
            status = result.get("filestatus")
            last_status = status
            if status == "SUCCESS":
                return int(result.get("total_pages") or 1)
            if status in ("FAIL", "FAILED", "ERROR"):
                raise SynapParseError(f"Synap 파싱 실패 (fid={fid}): {payload}")
            if time.monotonic() >= deadline:
                raise SynapParseError(
                    f"Synap 파싱 타임아웃 (fid={fid}, last_status={last_status})"
                )
            time.sleep(self.poll_interval)

    def _fetch_pages(self, fid: str, total_pages: int) -> List[str]:
        url = f"{self.engine}/result/{fid}"
        headers = {"Content-Type": "application/json"}
        pages: List[str] = []
        for i in range(max(total_pages, 1)):
            body = {
                "api_key": self.api_key,
                "page_index": i + 1,
                "type": self.output_type,
            }
            resp = requests.post(url, headers=headers, json=body, timeout=self.request_timeout)
            if resp.status_code != 200:
                raise SynapParseError(
                    f"Synap /result 실패 (fid={fid}, page={i + 1}, status={resp.status_code}): {resp.text[:200]}"
                )
            pages.append(resp.text)
        return pages

    def _delete(self, fid: str) -> None:
        try:
            requests.post(
                f"{self.engine}/delete/{fid}",
                json={"api_key": self.api_key},
                timeout=self.request_timeout,
            )
        except Exception as e:
            print(f"[synap] delete 실패 (fid={fid}): {e}")

    @staticmethod
    def _json_or_raise(resp: requests.Response, op: str) -> dict:
        if resp.status_code != 200:
            raise SynapParseError(
                f"Synap {op} HTTP {resp.status_code}: {resp.text[:200]}"
            )
        try:
            return resp.json()
        except ValueError as e:
            raise SynapParseError(f"Synap {op} 응답이 JSON이 아님: {resp.text[:200]}") from e
