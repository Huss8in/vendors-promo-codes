import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
import os

load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
print(EMAIL_USER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT, RECIPIENT_EMAIL)



def send_email_with_attachment(subject, body, to_email, attachments):
    try:
        # Set up the MIME
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Attach files
        for attachment in attachments:
            part = MIMEBase('application', 'octet-stream')
            with open(attachment, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment)}')
            msg.attach(part)

        # Send the email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            text = msg.as_string()
            server.sendmail(EMAIL_USER, to_email, text)
        print("Email sent successfully!")

    except Exception as e:
        print(f"Error sending email: {e}")

attachments = ['promo_data_csv/falseValidation.csv', 'promo_data_csv/trueRedemption.csv']

send_email_with_attachment(
    subject="Promo Data CSVs",
    body="Please find the attached promo data CSV files.",
    to_email=RECIPIENT_EMAIL,
    attachments=attachments
)
