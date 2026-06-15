"""Email channel adapter — SES SMTP with STARTTLS (04 §9).

We prefer SES *SMTP* over the SES API for portability (any SMTP provider drops
in). STARTTLS is required and a downgrade is rejected — we never send credentials
or alert content over a cleartext connection.

``smtplib`` is synchronous, so the blocking send runs in a worker thread via
``asyncio.to_thread`` to keep the dispatcher's event loop responsive. SMTP reply
codes classify cleanly: 4yz = transient (retry later), 5yz = permanent (04 §6).
"""

from __future__ import annotations

import asyncio
import smtplib
import ssl
from email.message import EmailMessage

from app.channels.base import Channel, DeliveryRequest, DeliveryResult
from app.channels.secrets import get_secret
from app.config import get_settings
from app.observability import get_logger

log = get_logger(__name__)


class SmtpDowngradeError(Exception):
    """Server did not advertise STARTTLS; we refuse to send in cleartext."""


class EmailChannel(Channel):
    name = "email"

    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        settings = get_settings()
        user = get_secret(settings.smtp_user_secret)
        password = get_secret(settings.smtp_password_secret)

        msg = EmailMessage()
        msg["From"] = settings.smtp_from
        msg["To"] = req.target
        msg["Subject"] = req.subject
        msg.set_content(req.message)

        try:
            await asyncio.to_thread(
                self._send_sync,
                host=settings.smtp_host,
                port=settings.smtp_port,
                user=user or "",
                password=password or "",
                msg=msg,
                timeout=self.policy.timeout_s,
            )
        except smtplib.SMTPAuthenticationError as exc:
            return DeliveryResult.permanent(f"auth failed: {exc.smtp_code}")
        except (smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused) as exc:
            return DeliveryResult.permanent(f"address refused: {exc!s}")
        except smtplib.SMTPResponseException as exc:
            # 4yz transient (greylisting, mailbox full), 5yz permanent.
            if 400 <= exc.smtp_code < 500:
                return DeliveryResult.transient(f"smtp {exc.smtp_code}")
            return DeliveryResult.permanent(f"smtp {exc.smtp_code}")
        except SmtpDowngradeError as exc:
            return DeliveryResult.permanent(str(exc))
        except (TimeoutError, OSError) as exc:
            # Connection reset / DNS / timeout — worth retrying.
            return DeliveryResult.transient(f"transport error: {exc!s}")

        return DeliveryResult.sent(provider_id=msg["Message-ID"] or None)

    @staticmethod
    def _send_sync(
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        msg: EmailMessage,
        timeout: float,
    ) -> None:
        context = ssl.create_default_context()
        with smtplib.SMTP(host, port, timeout=timeout) as smtp:
            smtp.ehlo()
            if not smtp.has_extn("starttls"):
                raise SmtpDowngradeError("server does not support STARTTLS; refusing cleartext")
            smtp.starttls(context=context)
            smtp.ehlo()
            if user:
                smtp.login(user, password)
            smtp.send_message(msg)
