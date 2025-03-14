"""
*************** Mailer
* Import den med 'from LK_emailer import Mailer'
* Opret et Mailer-objekt med 'mailer = Mailer()'
* Send en e-mail med 'mailer.sendmail(subject, tos, text, html, filename)'
"""

import smtplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.base import MIMEBase
from email import encoders
import json
import os.path

class Mailer:
    """
    En klasse til at sende e-mails med understøttelse af tekst, HTML, og vedhæftede filer.

    Mailer-klassen indlæser e-mailkonfiguration fra en JSON-fil og bruger SMTP til at sende e-mails.
    Den kan sende e-mails med både tekst- og HTML-indhold samt vedhæfte filer.

    Attributter:
    -----------
    __smtp_port : int
        Porten til SMTP-serveren, standard er 587.
    __smtp_server : str
        Adressen på SMTP-serveren.
    __sender_email : str
        Afsenderens e-mailadresse.
    __server_user : str
        Brugernavn til SMTP-serveren.
    __server_pass : str
        Adgangskode til SMTP-serveren.
    """

    def __init__(self) -> None:
        """
        Initialiserer Mailer-objektet ved at indlæse konfigurationsindstillinger fra en JSON-fil.
        """
        module_dir = os.path.dirname(__file__)
        args_path = os.path.join(module_dir, 'emailer_args.json')
        with open(args_path, 'r') as f:
            self.__args = json.load(f)
        self.__smtp_port = 587
        self.__smtp_server = self.__args['smtp_server']
        self.__sender_email = self.__args['sender']
        self.__server_user = self.__args['user']
        self.__server_pass = self.__args['pass']

    def __msg(self):
        """
        Opretter en MIMEMultipart-e-mailbesked med de angivne parametre.

        Returnerer:
        -----------
        MIMEMultipart
            En e-mailbesked med de relevante headers som 'Subject', 'From' og 'To'.
        """
        self.message = MIMEMultipart('alternative')
        self.message['Subject'] = self.subject
        self.message['From'] = self.__sender_email
        self.message['To'] = self.__receiver_emails
        return self.message
    
    def __attachment(self, filename):
        """
        Opretter en vedhæftet fil til e-mailen.

        Parametre:
        ----------
        filename : str
            Stien til filen, der skal vedhæftes.

        Returnerer:
        -----------
        MIMEBase
            En MIMEBase-objekt, der repræsenterer den vedhæftede fil.
        """
        if filename is not None:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open(filename, 'rb').read())
            email.encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(filename))
            return part

    def sendmail(self, subject: str, tos: list = None, text: str = None, html: str = None, filename: str = None):
        """
        Sender en e-mail med mulighed for at inkludere tekst, HTML og vedhæftede filer.

        Parametre:
        ----------
        subject : str
            Emnet for e-mailen.
        tos : list, valgfri
            En liste over modtagerens e-mailadresser. Hvis ingen er angivet, bruges afsenderens e-mail.
        text : str, valgfri
            E-mailens tekstindhold.
        html : str, valgfri
            E-mailens HTML-indhold.
        filename : str, valgfri
            Stien til en fil, der skal vedhæftes til e-mailen.
        """
        self.subject = subject
        self.tos = tos
        self.text = text
        self.html = html
        self.filename = filename
        if self.tos is not None:
            self.__receiver_emails = ', '.join(tos)
        else:
            self.__receiver_emails = self.__args['sender']
        msg = self.__msg()
        if self.text is not None:
            part1 = MIMEText(self.text, 'plain')
            msg.attach(part1)
        if self.html is not None:
            part2 = MIMEText(self.html, 'html')
            msg.attach(part2)

        if self.filename is not None:
            msg.attach(self.__attachment(filename))

        self.__send(msg)

    def __send(self, msg):
        """
        Sender den sammensatte e-mailbesked via SMTP.

        Parametre:
        ----------
        msg : MIMEMultipart
            Den e-mailbesked, der skal sendes.
        """
        self.msg = msg
        self.server = smtplib.SMTP(self.__smtp_server, self.__smtp_port)
        self.server.ehlo()
        self.server.starttls()
        self.server.ehlo()
        self.server.login(self.__server_user, self.__server_pass)
        self.server.sendmail(self.__sender_email, self.__receiver_emails, self.msg.as_string())
        self.server.close()
