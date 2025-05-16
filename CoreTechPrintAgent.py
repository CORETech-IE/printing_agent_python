import cx_Oracle
import os
import subprocess
import json
from cryptography.fernet import Fernet, InvalidToken
import logging
from logging.handlers import RotatingFileHandler
import time
import signal
import sys
from jsondiff import diff
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage


# import ipaddress
# import pyimod02_importers


class OracleConnection:
    def __init__(self, oracle_connection_index, oracle_connection_name, username, password, host, port, service,
                 oracle_retry_wait_time, connection_status, email_on_error, email_on_error_freq):
        self.oracle_connection_index = oracle_connection_index
        self.oracle_connection_name = oracle_connection_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service = service
        self.oracle_retry_wait_time = oracle_retry_wait_time
        self.connection_status = connection_status
        self.email_on_error = email_on_error
        self.connection = None
        self.last_connection_attempt = None
        self.last_email_attempt = None
        self.email_on_error_freq = email_on_error_freq
        self.last_error_message = None

    def connect(self, logger):
        dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service)
        try:
            self.connection = cx_Oracle.connect(self.username, self.password, dsn)
            self.connection_status = 'SUCCESS'
        except Exception as err:
            self.connection_status = 'NOT_SUCCESS'
            self.last_error_message = err.__str__()
            logger.error(
                'Unable to connect to Oracle Database ' + self.oracle_connection_name + ': ' + err.__str__())

    def close(self, logger):
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                self.connection_status = 'NOT_SUCCESS'
                logger.debug("Connection " + self.oracle_connection_name + " closed Successfully")
            except cx_Oracle.Error as error:
                logger.error("Connection " + self.oracle_connection_name + " Failed to Close" + " -> " + str(error))


# #######################################################################################################################
# ############################ END OF CLASS OracleConnection ############################################################
# #######################################################################################################################
class pyAgent:
    def __init__(self):
        # Initialize object variables to default values
        self.printer_name = None
        self.text_file = None
        self.current_rows_fetched = None
        self.out = None
        self.CurrentOracleConnection = None
        self.json_differences = None
        self.old_json_data = None
        self.logger_created = None
        self.handler_created = False
        self.loggerStats = None
        self.stats_file_name = "CoreTechPrintAgentStats.log"  # Fallback
        self.logger = None
        self.json_data = None
        self.oracle_connections = None
        self.encryption_key = b'w1GLAgxA5AK3DMcESVcdb166UcdZS4J31iIG0aNN8dw='  # Stored in CENT_SYS_CONFIGURATION 'CoreTechPrintAgent'/'Encryption KEY'
        self.cfg_logging_file_name = "CoreTechPrintAgent.log"  # Fallback
        self.cfg_execution_pause_time = 10  # in seconds. Fallback
        self.config_file_timestamp = None
        self.oracle_connections_list = []

    # Define a signal handler function
    def signal_handler(self, signal, frame):
        print(signal)

        self.close_all_oracle_connections()
        sys.exit(0)

    def add_oracle_connection(self, connection_index, connection_name, username, password, host, port, service,
                              oracle_retry_wait_time, connection_status, email_on_error, email_on_error_freq):
        connection = OracleConnection(connection_index, connection_name, username, password, host, port, service,
                                      oracle_retry_wait_time, connection_status, email_on_error, email_on_error_freq)
        # print(connection.oracle_connection_name)
        self.oracle_connections_list.append(connection)

    def connect_all_oracle_connections(self):
        for connection in self.oracle_connections_list:
            connection.connect(self.logger)

    def close_all_oracle_connections(self):
        for connection in self.oracle_connections_list:
            connection.close(self.logger)

    def setup_logger(self):

        logger = logging.getLogger('CoreTechPrintAgent')

        if self.json_data["main"]["logging_set_level"] == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        elif self.json_data["main"]["logging_set_level"] == 'WARNING':
            logger.setLevel(logging.WARNING)
        elif self.json_data["main"]["logging_set_level"] == 'ERROR':
            logger.setLevel(logging.ERROR)
        elif self.json_data["main"]["logging_set_level"] == 'CRITICAL':
            logger.setLevel(logging.CRITICAL)
        elif self.json_data["main"]["logging_set_level"] == 'INFO':
            logger.setLevel(logging.INFO)
        elif self.json_data["main"]["logging_set_level"] == 'NOTSET':
            logger.setLevel(logging.NOTSET)
        else:
            logger.setLevel(logging.NOTSET)

        if logger.hasHandlers():
            # Retrieve existing handlers
            existing_handlers = logger.handlers

            # Remove existing handlers
            for handler in existing_handlers:
                logger.removeHandler(handler)

        handler = RotatingFileHandler(self.json_data["main"]["logging_file_name"], mode='a',
                                      maxBytes=self.json_data["main"]["logging_max_file_size"],
                                      backupCount=self.json_data["main"]["logging_backup_count"])

        if self.json_data["main"]["logging_set_level"] == 'DEBUG':
            handler.setLevel(logging.DEBUG)
        elif self.json_data["main"]["logging_set_level"] == 'WARNING':
            handler.setLevel(logging.WARNING)
        elif self.json_data["main"]["logging_set_level"] == 'ERROR':
            handler.setLevel(logging.ERROR)
        elif self.json_data["main"]["logging_set_level"] == 'CRITICAL':
            handler.setLevel(logging.CRITICAL)
        elif self.json_data["main"]["logging_set_level"] == 'INFO':
            handler.setLevel(logging.INFO)
        elif self.json_data["main"]["logging_set_level"] == 'NOTSET':
            handler.setLevel(logging.NOTSET)
        else:
            handler.setLevel(logging.NOTSET)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def setup_logger_stats(self):
        logger = logging.getLogger('CoreTechPrintAgentStats')
        logger.setLevel(logging.DEBUG)

        # DELETE EXISTING HANDLERS
        if logger.hasHandlers():
            # Retrieve existing handlers
            existing_handlers = logger.handlers

            # Remove existing handlers
            for handler in existing_handlers:
                logger.removeHandler(handler)

        # CREATE NEW HANDLER
        handler = RotatingFileHandler(self.json_data["main"]["stats_file_name"], mode='a',
                                      maxBytes=self.json_data["main"]["stats_max_file_size"],
                                      backupCount=self.json_data["main"]["stats_backup_count"])
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s,%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        # header = '"Timestamp","Jobs Fetched","Jobs Printed","Jobs Failed"'

        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def setup_loggers(self):
        # Setup the loggers
        self.logger = self.setup_logger()
        self.logger.debug('CoreTechPrintAgent Logger Enabled ')

        self.loggerStats = self.setup_logger_stats()
        self.logger.debug('CoreTechPrintAgentStats Logger Enabled ')

    def read_config_json(self):

        try:
            with open('config.JSON', 'r') as file:

                # In case there was an old config, we store it to compare it
                if self.json_data is not None:
                    self.old_json_data = self.json_data

                self.json_data = None
                self.logger = None
                self.loggerStats = None
                self.logger_created = False
                self.handler_created = False

                # Get the Timestamp of the file
                self.config_file_timestamp = os.path.getmtime('config.JSON')
                # print(str(self.config_file_timestamp))

                # Parse the JSON data
                self.json_data = json.load(file)

                # Compare old and new JSON
                self.json_differences = diff(self.old_json_data, self.json_data)
                # print(self.json_differences)

        except FileNotFoundError as e:
            # Handle the error
            print(f"ERROR - Required Config File not found in ./ folder: {e.filename}")
            exit(1)

    def connect_to_db(self):
        # Create the Oracle database connections
        for oracle_connection_index, oracle_connection_data in self.json_data["oracle_connections"].items():
            try:
                self.add_oracle_connection(oracle_connection_index,
                                           oracle_connection_data["oracle_connection_name"],
                                           oracle_connection_data["oracle_username"],
                                           oracle_connection_data["oracle_password"],
                                           oracle_connection_data["oracle_host"],
                                           oracle_connection_data["oracle_port"],
                                           oracle_connection_data["oracle_service"],
                                           oracle_connection_data["oracle_retry_wait_time"],
                                           "NOT_SUCCESS",
                                           oracle_connection_data["email_on_error"],
                                           oracle_connection_data["email_on_error_freq"])

                # Connect and close the Oracle connections through OracleConnection Class
                self.connect_all_oracle_connections()

            except Exception as err:
                print(err.__str__())
                self.logger.error(
                    'Unable to connect to Oracle Database ' + oracle_connection_data[
                        "oracle_connection_name"] + ': ' + err.__str__())

            # print('STATUS after connection: ' + self.oracle_connections_list[0].connection_status)

    def connection_alive(self):

        for index, item_connection in enumerate(self.oracle_connections_list):
            if item_connection.connection_status == 'NOT_SUCCESS':

                # print('oracle_retry_wait_time:' + str(item_connection.oracle_retry_wait_time))
                # print('email_on_error_freq:' + str(item_connection.email_on_error_freq))
                # print (str(datetime.now()) + ' vs ' + str(item_connection.last_connection_attempt + timedelta(seconds=10)))

                if datetime.now() > item_connection.last_connection_attempt + timedelta(
                        seconds=int(item_connection.oracle_retry_wait_time)):
                    item_connection.last_connection_attempt = datetime.now()
                    item_connection.connect(self.logger)

                if datetime.now() > item_connection.last_email_attempt + timedelta(
                        seconds=int(item_connection.email_on_error_freq)):
                    # print('EMAIL!')
                    item_connection.last_email_attempt = datetime.now()
                    self.logger.debug(
                        "Sending email on Connection error to " + self.json_data["main"]["email_on_error"])
                    self.send_email_on_connection_error('pritning.agent@coretechnology.ie',
                                                        self.json_data["main"]["email_on_error"],
                                                        item_connection.oracle_connection_name,
                                                        item_connection.last_error_message)

    def print_to_ip_printer(self):
        # Construct the LPR command
        # lpr_command = ["lpr", "-S", self.printer_name, "-P", "lp", self.text_file]
        lpr_command = ["lpr", "-S", self.printer_name.split(',')[0], "-P", "lp", self.text_file]

        # Execute the LPR command
        try:
            self.out = subprocess.run(lpr_command, shell=True, check=True, capture_output=True, text=True)
            self.logger.debug("File sent to IP printer successfully.")
        except subprocess.CalledProcessError as e:
            # Handle the error here
            self.out = str(e)
            self.logger.error(f"Command execution failed with exit code {e.returncode}")

    def refresh_config_file(self):

        if self.config_file_timestamp < os.path.getmtime('config.JSON'):
            self.logger.debug(
                'Config File updated => Old Timestamp[' + str(self.config_file_timestamp) + '] vs New Timestamp[' + str(
                    os.path.getmtime('config.JSON')) + ']')
            return True

    def run(self):

        blob_data = None
        clob_data = None

        self.logger.debug('Run() - Process start')

        # Register the signal handler function for SIGINT
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        # Check for Sumatra app
        # Handle the "command not found" error here
        if not os.path.exists('SumatraPDF.exe'):
            self.logger.critical(
                "SumatraPDF.exe command not found in App root folder. Make sure it is in the App root folder.")
            print(
                "SumatraPDF.exe command not found in App root folder. Make sure it is in the App root folder.")
            exit(1)

        try:
            cx_Oracle.init_oracle_client(lib_dir=self.json_data["main"]["oracle_client"])
            self.logger.debug('Run() - Oracle Instance found')
        except Exception as err:
            self.logger.critical("Unable to find the Oracle Instance " + err.__str__())
            print('ERROR - Unable to find the Oracle Instance in folder: ' + self.json_data["main"]["oracle_client"])
            exit(1)

        # Connect to all Databases
        self.connect_to_db()

        # after first time connection attempt, we can store the time to use it later so send emails
        for index, item_connection in enumerate(self.oracle_connections_list):
            item_connection.last_connection_attempt = datetime.now()
            item_connection.last_email_attempt = datetime.now()

        while True:

            # Check for changes in the Config File
            if self.refresh_config_file():
                self.logger.debug('Config Data was updated. Proceed to reload config...')
                # Close existing Connections
                self.logger.debug('Closing existing Database Connections for safety...')
                self.close_all_oracle_connections()
                # Reset the connections List
                self.oracle_connections_list = []
                # Read Config File again
                self.logger.debug('Reading the new configuration from file...')
                self.read_config_json()
                self.setup_loggers()
                self.logger.info('Config Changes in file: ' + str(self.json_differences))
                self.logger.debug('Decrypting credentials...')
                # Decrypt data
                self.decrypt_credentials()
                # Connect to DBs
                self.logger.debug('Reconnecting to Database ...')
                self.connect_to_db()
                continue

            self.logger.info('Number of Config Database Connections: ' + str(len(self.oracle_connections_list)))

            # Now we have the connections, so loop on Connections and execute the queries
            for index, item_connection in enumerate(self.oracle_connections_list):

                # Check if connection is alive. If it is not, try to reconnect
                self.connection_alive()

                # print(item_connection.last_connection_attempt)

                # If a connection is down, we don't try to run any query
                if item_connection.connection_status == 'NOT_SUCCESS':
                    continue

                # Open a new cursor
                cursor = item_connection.connection.cursor()
                # print(self.json_data["oracle_connections"][str(index)]["oracle_connection_name"])
                self.logger.debug(
                    'Run() - Prepare Query in Database ' + self.json_data["oracle_connections"][str(index)][
                        "oracle_connection_name"])

                # Retrieve rows from cent_iface_print table where cip_processed = 'N'
                query = "SELECT cip_blob, cip_id, cip_file_id, cip_printer_name, cip_clob, cip_printer_category FROM cent_iface_print WHERE nvl(cip_process_ind, 'N') = 'N' "
                cursor.execute(query)

                self.logger.debug('Run() - Query Executed')

                rows = cursor.fetchall()

                # Get the number of rows fetched
                self.current_rows_fetched = str(cursor.rowcount)
                # print("Fetching " + str(current_rows_fetched) + " rows")
                self.logger.debug('Number of rows fetched to be printed: ' + self.current_rows_fetched)

                # Process each row
                for row in rows:

                    if row[0] is not None:
                        blob_data = row[0].read()  # Assuming cip_blob_file is of BLOB type

                    row_cip_id = row[1]
                    temp_file = row[2]
                    self.printer_name = row[3]

                    if row[4] is not None:
                        clob_data = row[4]

                    # TEXT or LASER
                    printer_type = row[5]

                    # create the folder in advance if it doesn't exit
                    if not os.path.exists('temp\\'):
                        try:
                            self.logger.warning(f"Folder temp\\ does not exist. Needs to be created.")
                            os.makedirs('temp\\')
                            self.logger.warning(f"Folder temp\\ created.")
                        except FileExistsError:
                            pass  # Ignore the error if the directory already exists

                    if printer_type == 'LASER':
                        # Create a file-like object from the BLOB data
                        # Save the BLOB data to a file
                        with open('temp\\' + temp_file, 'wb') as file:
                            file.write(blob_data)
                        self.logger.debug(f"File saved to temp\\" + temp_file + " successfully.")

                        # Print the file using SumatraPDF
                        try:
                            print_command = ['SumatraPDF.exe', '-silent', '-print-to', self.printer_name,
                                             'temp\\' + temp_file]
                            self.out = subprocess.run(print_command, shell=True, capture_output=True, text=True)
                            self.logger.debug(f"File {temp_file} sent to the printer successfully.")
                            self.logger.debug(self.out)
                        except Exception as e:
                            # Handle the error here
                            self.logger.error(f"Command execution failed with exit code {e}")
                            self.logger.error(f"Error output: {e}")

                        # Delete the file after successful printing
                        os.remove('temp\\' + temp_file)
                        self.logger.debug(f"File {temp_file} deleted.")

                    elif printer_type == 'TEXT':
                        # Save the CLOB data to a file
                        self.text_file = 'temp\\' + temp_file + '_' + str(row_cip_id) + '.txt'

                        # print(clob_data)

                        with open(self.text_file, "w", encoding="ISO-8859-1") as file:
                            # Write the CLOB data to the file
                            file.write(clob_data.read().replace('\n', ''))

                        # Send it to the Printer through LPR
                        self.print_to_ip_printer()

                        # Delete the file after successful printing
                        os.remove(self.text_file)
                        self.logger.debug(f"File {self.text_file} deleted.")

                    # Update the row as cip_processed = 'Y'
                    # Open a new cursor

                    cursor = item_connection.connection.cursor()
                    cursor.execute(
                        "UPDATE cent_iface_print SET cip_process_ind = 'Y', cip_process_error = substr(:msg, 1, 250) WHERE cip_id = :id",
                        id=row_cip_id, msg=str(self.out))
                    item_connection.connection.commit()
                    self.logger.debug(f"Row {row_cip_id} updated to Printed Status.")

                    # Close the cursor
                    cursor.close()

            # Log some stats
            self.loggerStats.debug(
                f"{self.current_rows_fetched},{self.current_rows_fetched},{self.current_rows_fetched}")

            # Sleep for a defined period of seconds or the fallback
            self.logger.debug("Sleeping for " + str(self.json_data["main"]["execution_pause_time"]) + " seconds")
            time.sleep(self.json_data["main"]["execution_pause_time"] or self.cfg_execution_pause_time)

    def decrypt_credentials(self):
        # Access the oracle connection instances and their properties
        # print(str(len(self.json_data["oracle_connections"].items())))

        # Create a Fernet instance with the encryption key
        fernet = Fernet(self.encryption_key)

        for oracle_connection_name, oracle_connection_data in self.json_data["oracle_connections"].items():
            # Decrypt user and password into memory
            try:
                # print(str(oracle_connection_data["oracle_username"]))
                oracle_connection_data["oracle_username"] = fernet.decrypt(
                    oracle_connection_data["oracle_username"].encode()).decode()
                self.logger.info('User Name for Connection ' + oracle_connection_data[
                    "oracle_connection_name"] + ' Decrypted')
            except InvalidToken as e:
                self.logger.critical("Error: Invalid token in Username for Connection " + oracle_connection_data[
                    "oracle_connection_name"] + ' -> ' + e.__str__())
                exit(1)

            try:
                oracle_connection_data["oracle_password"] = fernet.decrypt(
                    oracle_connection_data["oracle_password"].encode()).decode()
                self.logger.info('Password for Connection ' + oracle_connection_data[
                    "oracle_connection_name"] + ' Decrypted')
            except InvalidToken as e:
                self.logger.critical("Error: Invalid token in Password for Connection " + oracle_connection_data[
                    "oracle_connection_name"] + " -> " + e.__str__())
                exit(1)

    def send_email_on_connection_error(self, sender, receiver, connection_name, error_message):

        subject = 'CoreTechPrintAgent ERROR - ' + self.json_data["main"]["client_name"]
        body = 'Unable to connect to database - ' + connection_name + ' -> ' + error_message
        message = EmailMessage()
        message['From'] = sender
        message['To'] = receiver
        message['Subject'] = subject
        message.set_content(body)

        # Set the SMTP server and port
        smtp_server = self.json_data["main"]["email_server"]
        smtp_port = 25  # Replace with the appropriate port number

        # Connect to the SMTP server and send the email
        self.logger.debug('Sending email...')

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.send_message(message)
                self.logger.debug("Email sent to " + self.json_data["main"]["email_on_error"])
        except Exception as e:
            self.logger.error("An error occurred while sending the email: ", str(e))


def main():
    # Create an instance of the class
    agent = pyAgent()

    # Read the config file
    agent.read_config_json()

    # Setup Loggers
    agent.setup_loggers()

    # Decrypt data
    agent.decrypt_credentials()

    # Call procedures on the instance
    agent.run()  # Call the printing process


if __name__ == "__main__":
    main()
