"""
python logging_example.py

Best practice for logging any python scripts
"""

# *********************************************************************************************************************

# standard imports
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from datetime import datetime

# 3rd party imports
import click

@click.command()
@click.option('-f', '--log_folder_path', type=str, required=False, default = "./data/", help='Log folder path')
@click.option('-m', '--max_bytes', type=int, required=False, default = 1073741824,
              help='Max size of a log file in bytes. Default is 1GB')
@click.option('-l', '--log_level',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'critical']),
              default='info',
              help='Log levels')
def main(max_bytes: int,
         log_folder_path: str,
         log_level: str) -> None:
    """Main Function"""

    # set log level
    if log_level == "debug":
        log_level = logging.DEBUG
    elif log_level == "info":
        log_level = logging.INFO
    elif log_level == "warning":
        log_level = logging.WARNING
    elif log_level == "error":
        log_level = logging.ERROR
    elif log_level == "critical":
        log_level = logging.CRITICAL
    else:
        raise RuntimeError("Incorrect log level. Should be one of debug, info, warning, error, critical")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_folder_path,f"{timestamp}.log")
    setup_logging(log_file_path, log_level, max_bytes)

    # Redirect stdout and stderr
    redirect_output_to_log(log_file_path)

    # Create a logger object
    logger = logging.getLogger(__name__)

    # Log some messages
    for i in range(100):
        logger.debug('This is log message number %d', i)
        logger.info('This is log message number %d', i)
        logger.warning('This is log message number %d', i)
        logger.error('This is log message number %d', i)
        logger.critical('This is log message number %d', i)

    print("PRINT STATEMENT")

def setup_logging(log_file_path: str, log_level: logging, max_bytes: int):
    """Set up logging"""

    # Remove all existing handlers
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # When the log file size exceeds 1GB. Automatically a new file is created and old one is saved
    # Can go to upto 4 additional log files
    # If the number of log files exceed the backup count + 1, then automatically the oldest log file is deleted
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            RotatingFileHandler(filename=log_file_path,
                                mode='a',
                                maxBytes=max_bytes, # 1GB per log file limit
                                backupCount=4,
                                encoding="utf8"),
            # Remove StreamHandler to prevent double logging
            # logging.StreamHandler()  # You can comment this out if you don't want logs in the terminal
        ]
    )

    # Overwrite GRPC loggings
    # # Configure grpc logging
    # grpc_logger = logging.getLogger('grpc')
    # grpc_logger.setLevel(log_level)
    # file_handler = RotatingFileHandler(filename=log_file_path,
    #                                    mode='a',
    #                                    maxBytes=1073741824, # 1GB per log file limit
    #                                    backupCount=4,
    #                                    encoding="utf8")
    # file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    # grpc_logger.addHandler(file_handler)


def redirect_output_to_log(log_file_path):
    """Redirect output to log"""
    # Redirect stdout and stderr to the log file
    log_file = open(log_file_path, 'a', encoding="utf8")
    os.dup2(log_file.fileno(), sys.stdout.fileno())
    os.dup2(log_file.fileno(), sys.stderr.fileno())

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
