import asyncio
from types import SimpleNamespace

from dvorik.bot.handlers import supply_upload as supply


class _DummyState:
    async def get_data(self):
        return {"expect_excel": True}


class _DummyMessage:
    def __init__(self, document):
        self.document = document
        self.answers = []

    async def answer(self, text, reply_markup=None):
        self.answers.append({"text": text, "reply_markup": reply_markup})

    @property
    def bot(self):  # pragma: no cover - should not be accessed in this test
        raise AssertionError("bot should not be accessed for invalid documents")


def test_resolve_file_name_without_original_name_uses_mime_type():
    document = SimpleNamespace(file_name=None, mime_type="text/csv", file_id="FAKE123")

    resolved = supply._resolve_file_name(document)

    assert resolved.startswith("upload")
    assert resolved.endswith(".csv")


def test_on_document_without_file_name_reports_format_error():
    document = SimpleNamespace(file_name=None, mime_type="application/pdf", file_id="PDF123")
    message = _DummyMessage(document)
    state = _DummyState()

    asyncio.run(supply.on_document(message, state))

    assert message.answers, "handler should report format error"
    first_reply = message.answers[0]["text"]
    assert "CSV" in first_reply and "Excel" in first_reply
