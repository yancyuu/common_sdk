import imaplib
import smtplib
from email.mime.text import MIMEText
import email
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
        status, messages = self.imap.search(encoding, key)
        # 处理邮件的代码可以放在这里
        return messages[0].split()  # 将字符串分割成列表


    def fetch_emails(self, encoding=None, key="UNSEEN"):
        """
            @param: encoding None 参数指定搜索条件的字符集。在这里，None 表示不使用任何特定的字符集。这是一个常见的做法，因为很多IMAP服务器默认使用UTF-8或相应的字符集。
            @param：key 'ALL' 参数是一个搜索关键字，它告诉服务器返回所有邮件的列表。IMAP搜索关键字可以很灵活，比如 'UNSEEN' 用于搜索所有未读邮件，'FROM "example@example.com"' 用于搜索来自特定地址的邮件等。
        """
        stripper = MLStripper()
        email_boxs = []
        mail_ids = self.fetch_email_ids(encoding, key)
        for mail_id in mail_ids:
            # data 是一个包含邮件数据的复杂结构
            status, data = self.imap.fetch(mail_id, '(RFC822)')
            # 通常，邮件内容包含在 data 列表的第一部分
            for response_part in data:
                if isinstance(response_part, tuple):
                    # 将邮件内容解析为邮件对象
                    msg = email.message_from_bytes(response_part[1])
                    # 从邮件对象中提取所需信息，例如主题、发件人、邮件正文等
                    email_subject = msg['subject']
                    if email_subject is None:
                        email_subject = "无主题"
                    email_from = msg['from']
                    print('From : ' + email_from + '\n')
                    print('Subject : ' + email_subject + '\n')
                    # 使用 get_body 函数提取邮件正文
                    email_body = self.get_body(msg)
                    if 'html' in msg.get_content_type():
                        email_text = stripper.get_text_from_html(email_body)
                    else:
                        email_text = email_body
                    if not email_text:
                        continue
                    print('Email_text : ' + email_text + '\n')
                    email_boxs.append({"subject": email_subject, "from": email_from, "text": email_text})
        # 处理邮件的代码可以放在这里
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

