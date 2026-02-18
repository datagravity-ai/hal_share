import datetime
import importlib
import logging
import os
from contextlib import redirect_stderr, redirect_stdout

import azure.functions as func


# path to the anomalo-catalog.py file in the root of this package
integration_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "anomalo-catalog.py"
)


class LoggerWriter:
    def __init__(self, level):
        """
        level: The logging level (e.g., logging.INFO, logging.ERROR).
        """
        self.level = level
        self.buffer = ""

    def write(self, message):
        self.buffer += message
        # Process lines when a newline character is received
        if "\n" in self.buffer:
            lines = self.buffer.splitlines(True)  # Keep newlines in the output
            for line in lines:
                if line.endswith("\n"):
                    logging.log(self.level, line.rstrip("\n"))
                else:
                    # This part of the message is an incomplete line, re-buffer it
                    self.buffer = line
                    break  # Stop processing because end of buffer is incomplete message
            else:
                self.buffer = ""

    def flush(self):
        # ensure any partially buffered message is logged on flush
        if self.buffer:
            logging.log(self.level, self.buffer)
            self.buffer = ""


async def main(anomalo_timer: func.TimerRequest) -> None:
    if anomalo_timer.past_due:
        logging.info("The timer is past due!")

    logging.info(
        f"Anomalo Catalog Sync triggered at {datetime.datetime.now().isoformat()}"
    )
    logging.info(f"""Configuration:
ANOMALO_INSTANCE_HOST = {os.environ.get("ANOMALO_INSTANCE_HOST", "app.anomalo.com")}
ANOMALO_API_SECRET_TOKEN = {os.environ.get("ANOMALO_API_SECRET_TOKEN", "")[:6]}...
ANOMALO_ORGANIZATION_ID = {os.environ.get("ANOMALO_ORGANIZATION_ID")}

TIMER_SCHEDULE_CRON = {os.environ.get("TIMER_SCHEDULE_CRON")}

CLI_ARGS = {os.environ.get("CLI_ARGS")}
""")

    CLI_ARGS = os.environ.get("CLI_ARGS", "").split(" ")
    if not CLI_ARGS:
        logging.error(
            "No CLI arguments provided. Please set the CLI_ARGS environment variable."
        )
        return

    logging.info(f"Starting integration from: {integration_path}")
    try:
        if not os.path.exists(integration_path):
            logging.error(
                f"Anomalo catalog integration not found at {integration_path}"
            )
            return

        original_cwd = os.getcwd()

        logging.info(f"Anomalo catalog integration execution: starting")

        # Redirect stdout and stderr to the logger
        stdout_logger_writer = LoggerWriter(logging.INFO)  # Log stdout as info
        stderr_logger_writer = LoggerWriter(logging.ERROR)  # Log stderr as error
        with (
            redirect_stdout(stdout_logger_writer),
            redirect_stderr(stderr_logger_writer),
        ):
            os.chdir(os.path.dirname(integration_path))
            logging.info(f"Changed CWD to: {os.getcwd()}")

            logging.info("Loading catalog integration")
            catalog = importlib.import_module("anomalo-catalog")

            logging.info("Invoking catalog sync")
            catalog.main(CLI_ARGS)

            stdout_logger_writer.flush()
            stderr_logger_writer.flush()

        logging.info(f"Catalog execution: complete")
    except Exception as e:
        logging.error(f"Error executing integration: {e}", exc_info=True)
        try:
            stdout_logger_writer.flush()
            stderr_logger_writer.flush()
        except Exception as flush_error:
            logging.error(f"Error flushing logs: {flush_error}", exc_info=True)
    finally:
        os.chdir(original_cwd)
        logging.info(f"Restored CWD to: {original_cwd}")

    logging.info("Catalog integration run finished.")
