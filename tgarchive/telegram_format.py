import re
from html import escape


class TelegramFormatter:
    """
    Convert Telegram-style formatting to CommonMark / HTML-safe Markdown
    """

    BOLD = re.compile(r"\*(.*?)\*")
    ITALIC = re.compile(r"_(.*?)_")
    STRIKE = re.compile(r"~(.*?)~")
    UNDERLINE = re.compile(r"__(.*?)__")
    INLINE_CODE = re.compile(r"`([^`]+)`")

    def convert(self, text: str) -> str:
        if not text:
            return ""

        # Escape HTML first to avoid XSS
        text = escape(text)

        # Telegram underline → HTML (CommonMark has no underline)
        text = self.UNDERLINE.sub(r"<u>\1</u>", text)

        # Telegram → CommonMark
        text = self.BOLD.sub(r"**\1**", text)
        text = self.ITALIC.sub(r"*\1*", text)
        text = self.STRIKE.sub(r"~~\1~~", text)
        text = self.INLINE_CODE.sub(r"`\1`", text)

        return text
