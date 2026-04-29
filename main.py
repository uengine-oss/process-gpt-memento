"""진입점 — `python main.py`로 실행."""
# 모듈 import 시점에는 로깅 셋업을 하지 않는다. multiprocessing의 spawn 자식이
# 이 모듈을 재 import할 때 로그 파일이 truncate되는 사고를 막기 위함.

if __name__ == "__main__":
    from app.core.logging_setup import setup_file_logging

    setup_file_logging("memento.log")

    from app.main import app
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8005)
else:
    from app.main import app  # noqa: F401  — uvicorn이 "main:app" 문자열로 부를 때 대비
