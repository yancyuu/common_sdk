import imaplib
import smtplib
from email.mime.text import MIMEText
from ..system.sys_env import get_env


class EmailClient:
    def __init__(self):
        self.imap_host = get_env("IMAP_HOST")
        self.smtp_host = get_env("SMTP_HOST")
        self.username = get_env("EMAIL_ACCOUNT_NAME")
        self.password = get_env("EMAIL_ACCOUNT_PASS")

    def connect_imap(self):
        self.imap = imaplib.IMAP4(self.imap_host, 143)
        self.imap.login(self.username, self.password)
        self.imap.select('inbox')

    def fetch_emails(self):
        status, messages = self.imap.search(None, 'ALL')
        # 处理邮件的代码可以放在这里
        return messages

    def close_imap(self):
        self.imap.close()
        self.imap.logout()

    def send_email(self, to_addr, subject, body):
        smtp = smtplib.SMTP(self.smtp_host, 25)
        smtp.starttls()  # 如果服务器支持TLS
        smtp.login(self.username, self.password)

        message = MIMEText(body, 'plain', 'utf-8')
        message['From'] = self.username
        message['To'] = to_addr
        message['Subject'] = subject

        smtp.sendmail(self.username, to_addr, message.as_string())
        smtp.quit()

