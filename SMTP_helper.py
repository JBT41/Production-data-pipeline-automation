
import smtplib
from email.message import EmailMessage
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = os.getenv("SMTP_PORT")


from_address = os.getenv("FROM_ADDRESS")
to_address = os.getenv("TO_ADDRESS")



def send(
    success: bool,
    *,
    error: Exception | None = None,
    sdyr: int | None = None,
    sdwk: int | None = None,
    rows_loaded: int | None = None
):
    msg = EmailMessage()
    msg["From"] = from_address
    msg["To"] = to_address

    status = "SUCCESS" if success else "FAILED"

    msg["Subject"] = (
        f"Web Visit Upload {status}"
        + (f" | SDYR {sdyr} SDWK {sdwk}"
           if sdyr is not None and sdwk is not None else "")
    )

    lines = [
        f"Status: {status}",
        f"Run time: {datetime.now()}",
    ]

    if sdyr is not None and sdwk is not None:
        lines.append(f"Fiscal Period: SDYR {sdyr}, SDWK {sdwk}")

    if rows_loaded is not None:
        lines.append(f"Rows loaded: {rows_loaded}")

    if not success and error is not None:
        lines.extend([
            "",
            "Error:",
            str(error),
            "",
            "Traceback:",
            traceback.format_exc(),
        ])

    msg.set_content("\n".join(lines))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.send_message(msg)
