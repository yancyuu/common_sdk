import imaplib
import smtplib
import email
from email.mime.text import MIMEText
from email.header import decode_header
from html.parser import HTMLParser
import re

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = []

    def handle_data(self, d):
        self.text.append(d)

    def get_data(self):
        return ''.join(self.text)

    def strip_tags(self, html):
        self.feed(html)
        return self.get_data()

    def get_text_from_html(self, html_body):
        # 移除HTML标签
        text = self.strip_tags(html_body)
        # 可能还需要进一步清理，如移除多余的空白字符
        text = re.sub(r'\s+', ' ', text).strip()
        return text


class IMAPEmailClient:
    def __init__(self, imap_host, imap_port, smtp_host, smtp_port, account_name, account_password):
        self.imap_host = imap_host
        self.imap_port = imap_port
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = account_name
        self.password = account_password
        self.connect_imap()

    def connect_imap(self):
        self.imap = imaplib.IMAP4(self.imap_host, self.imap_port)
        self.imap.login(self.username, self.password)
        self.imap.select('inbox')
    

    
    def get_body(self, message):
        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))

                if content_type == "text/plain" and "attachment" not in content_disposition:
                    try:
                        return part.get_payload(decode=True).decode('utf-8')
                    except UnicodeDecodeError:
                        return part.get_payload(decode=True).decode('latin1')  # 尝试使用另一种编码
        else:
            try:
                return message.get_payload(decode=True).decode('utf-8')
            except UnicodeDecodeError:
                return message.get_payload(decode=True).decode('latin1')
    
    def fetch_email_ids(self, encoding=None, key='BODY "invoice"'):
        """
            @param: encoding None 参数指定搜索条件的字符集。在这里，None 表示不使用任何特定的字符集。这是一个常见的做法，因为很多IMAP服务器默认使用UTF-8或相应的字符集。
            @param：key 'ALL' 参数是一个搜索关键字，它告诉服务器返回所有邮件的列表。IMAP搜索关键字可以很灵活，比如 'UNSEEN' 用于搜索所有未读邮件，'FROM "example@example.com"' 用于搜索来自特定地址的邮件等。
        """
        # 根据给定的关键字搜索邮件，返回邮件 ID 列表
        typ, data = self.imap.search(None, key)
        if typ != 'OK':
            return []
        return data[0].split()


    def fetch_emails(self, encoding='utf-8', key="UNSEEN"):
        """
        @param encoding: 参数指定搜索条件的字符集。默认为 UTF-8。
        @param key: 搜索关键字，如 'UNSEEN' 表示所有未读邮件。
        """
        stripper = MLStripper()
        email_boxs = []
        self.imap.encoding = encoding

        # 搜索邮件
        if not all(ord(c) < 128 for c in key):
            key = key.encode(encoding)
        mail_ids = self.fetch_email_ids(encoding, key)
        
        for mail_id in mail_ids:
            typ, data = self.imap.fetch(mail_id, '(RFC822)')
            if typ != 'OK':
                continue
            for response_part in data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    email_subject = "无主题"
                    if msg['subject']:
                        email_subject = decode_header(msg['subject'])
                        email_subject = ''.join([text.decode(charset or 'utf-8') if charset else text for text, charset in email_subject])
                    email_from = msg['from']
                    email_body = self.get_body(msg)
                    email_text = stripper.get_text_from_html(email_body) if 'html' in msg.get_content_type() else email_body
                    if email_text:
                        email_boxs.append({"subject": email_subject, "from": email_from, "text": email_text})

        return email_boxs

    def close_imap(self):
        self.imap.close()
        self.imap.logout()

    def send_email(self, to_addr, subject, body, type ="plain", encoding ="utf-8"):
        smtp = smtplib.SMTP(self.smtp_host, self.smtp_port)
        smtp.starttls()  # 如果服务器支持TLS
        smtp.login(self.username, self.password)
        message = MIMEText(body, type, encoding)
        message['From'] = self.username
        message['To'] = to_addr
        message['Subject'] = subject

        smtp.sendmail(self.username, to_addr, message.as_string())
        smtp.quit()

