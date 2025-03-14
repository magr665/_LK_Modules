"""
********** Logger **********
* Import den med 'from LK_logger import Logger'
* Opret et Logger-objekt med 'logger = Logger(path, debug, date_at_end, filename, num_of_logs, print_msg)'
* Start logningen med 'logger.start()'
* Log en informationsbesked med 'logger.info(msg)'
* Log en advarselsbesked med 'logger.warning(msg)'
* Log en kritisk besked med 'logger.critical(msg)'
* Afslut logningen med 'logger.end()'
* Tjek loggen for advarsler og/eller kritiske fejl med 'logger.checklog(type)'

"""
import os
import datetime
import time
import glob
from time import perf_counter

class Logger:
    """
    En klasse til at logge beskeder og håndtere logfiler.

    Klassen Logger giver funktionalitet til at oprette logfiler, logge forskellige typer af beskeder 
    (info, advarsler, kritiske fejl), og holde styr på antallet af logfiler. Den understøtter også 
    debug-tilstand og kan udskrive beskeder direkte til konsollen.

    Attributter:
    -----------
    path : str
        Stien til mappen, hvor logfilen skal oprettes.
    debug : bool
        Aktiverer eller deaktiverer debug-tilstand (standard: False).
    date_at_end : bool
        Tilføjer dato og tid til filnavnet på logfilen (standard: False).
    filename : str, valgfri
        Navnet på logfilen (uden udvidelse). Hvis ikke angivet, bruges 'log' som standard.
    num_of_logs : int
        Antal logfiler, der skal gemmes. Overskydende filer slettes automatisk (standard: 10).
    print_msg : bool
        Udskriver logbeskeder direkte til konsollen (standard: False).
    """

    def __init__(self, path, debug=False, date_at_end=False, filename=None, num_of_logs=10, print_msg=False):
        """
        Initialiserer Logger-objektet og opretter logfilen.

        Parametre:
        ----------
        path : str
            Stien til mappen, hvor logfilen skal oprettes.
        debug : bool, valgfri
            Aktiverer eller deaktiverer debug-tilstand (standard: False).
        date_at_end : bool, valgfri
            Tilføjer dato og tid til filnavnet på logfilen (standard: False).
        filename : str, valgfri
            Navnet på logfilen (uden udvidelse). Hvis ikke angivet, bruges 'log' som standard.
        num_of_logs : int, valgfri
            Antal logfiler, der skal gemmes. Overskydende filer slettes automatisk (standard: 10).
        print_msg : bool, valgfri
            Udskriver logbeskeder direkte til konsollen (standard: False).
        """
        self.path = path
        self.start_time = perf_counter()
        self.__warnings = 0
        self.__criticals = 0
        self.debug = debug
        self.__date_at_end = date_at_end
        self.__filename = filename
        self.__num_of_logs = num_of_logs
        self.__print = print_msg
        if not self.__filename:
            self.__filename = 'log'
        if self.__date_at_end == True:
            self.__check_num_of_files()
            self.__filename = f"{self.__filename} {self.__get_time().replace(':','.')}.log"
        else:
            self.__filename = f"{self.__filename}.log"
        self.path = os.path.join(path, self.__filename)

    def __get_time(self):
        """
        Henter den aktuelle dato og tid i formatet 'ÅÅÅÅ-MM-DD TT:MM:SS'.

        Returnerer:
        -----------
        str
            Den aktuelle dato og tid som en streng.
        """
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def __write(self, msg):
        """
        Skriver en besked til logfilen og eventuelt til konsollen.

        Parametre:
        ----------
        msg : str
            Beskeden, der skal logges.
        """
        self.msg = msg
        if self.__print == True:
            print(self.msg)
        with open(self.path, 'a', encoding='utf-8') as file:
            file.write(self.msg + '\n')

    def __check_num_of_files(self):
        """
        Kontrollerer antallet af logfiler i mappen og sletter overskydende filer, så kun det angivne antal gemmes.
        """
        log_files = sorted(
            glob.glob(os.path.join(self.path, f'{self.__filename}*.log')),
            key=os.path.getmtime,
            reverse=True
        )

        files_to_delete = log_files[self.__num_of_logs:]
        for file_path in files_to_delete:
            os.remove(file_path)

        return

    def start(self):
        """
        Starter logningen og skriver en startbesked til logfilen.

        Hvis debug-tilstand er aktiveret, skrives yderligere debug-info til logfilen.
        """
        self.msg = f"{self.__get_time()} : INFO     : Starting"
        with open(self.path, 'w', encoding='utf-8') as file:
            file.write(self.msg + '\n')
        if self.debug == True:
            self.info('*****************************')
            self.info('******** DEBUGGING **********')
            self.info('*****************************')

    def info(self, msg):
        """
        Logger en informationsbesked.

        Parametre:
        ----------
        msg : str
            Beskeden, der skal logges som info.
        """
        self.msg = f"{self.__get_time()} : INFO     : {msg}"
        self.__write(self.msg)

    def critical(self, msg):
        """
        Logger en kritisk besked.

        Hvis debug-tilstand er deaktiveret, øges tælleren for kritiske fejl.

        Parametre:
        ----------
        msg : str
            Beskeden, der skal logges som kritisk.
        """
        self.msg = f"{self.__get_time()} : CRITICAL : {msg}"
        self.__write(self.msg)
        if self.debug == False:
            self.__criticals += 1

    def warning(self, msg):
        """
        Logger en advarselsbesked.

        Hvis debug-tilstand er deaktiveret, øges tælleren for advarsler.

        Parametre:
        ----------
        msg : str
            Beskeden, der skal logges som advarsel.
        """
        self.msg = f"{self.__get_time()} : WARNING  : {msg}"
        self.__write(self.msg)
        if self.debug == False:
            self.__warnings += 1

    def end(self):
        """
        Afslutter logningen og skriver en afslutningsbesked samt tidsforbrug til logfilen.

        Oplyser også om eventuelle advarsler og kritiske fejl, der er registreret.
        """
        elapsed_time = round(self.endTime())
        elapsed = time.strftime('%H:%M:%S', time.gmtime(elapsed_time))
        msgs = []
        msgs.append(f"{self.__get_time()} : INFO     : Ending")
        msgs.append(f"{self.__get_time()} : INFO     : Time used : {elapsed.split('.')[0]}")
        if self.__warnings == 1: 
            warning_txt = 'Warning'
        else:
            warning_txt = 'Warnings'
        if self.__criticals == 1:
            critical_txt = 'Critical'
        else:
            critical_txt = 'Criticals'
        msgs.append(f"{self.__get_time()} : INFO     : Ended with {self.__warnings} {warning_txt} and {self.__criticals} {critical_txt}")
        for msg in msgs:
            self.__write(msg)

    def endTime(self):
        """
        Beregner den forløbne tid siden logningen startede.

        Returnerer:
        -----------
        float
            Den forløbne tid i sekunder.
        """
        return perf_counter() - self.start_time

    def checklog(self, type='both') -> bool:
        """
        Kontrollerer loggen for kritiske fejl og/eller advarsler.

        Parametre:
        ----------
        type : str, valgfri
            Vælg mellem 'criticals', 'warnings', 'both'. Returnerer False, hvis nogen af de valgte typer er registreret, ellers True.

        Returnerer:
        -----------
        bool
            True, hvis ingen af de valgte typer er registreret; ellers False.
        """
        self.type = type.lower()
        if self.type not in ['criticals', 'warnings', 'both']:
            self.critical('Wrong check type')
            return False
        if self.type == 'criticals' and self.__criticals > 0:
            return False
        elif self.type == 'warnings' and self.__warnings > 0:
            return False
        elif self.type == 'both' and (self.__criticals > 0 or self.__warnings > 0):
            return False        
        else:
            return True      
