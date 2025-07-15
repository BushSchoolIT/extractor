import os
import sys
import io
import subprocess
import pathlib
from prefect import flow, serve, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

EXTRACTOR_PATH = str(pathlib.Path(__file__) / "bbextract")
MAILSYNC_PATH = str(pathlib.Path(__file__) / "mailsync")

def runExe(args: list[str]) -> dict:
    exe_path = args[0]
    exe_dir = os.path.dirname(exe_path)
    name = os.path.basename(exe_path)
    logger = get_run_logger()

    try:
        result = subprocess.run(
            [exe_path] + args[1:],
            cwd=exe_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW
        )

        output = result.stdout.decode("utf-8", errors="replace").strip()
        error_output = result.stderr.decode("utf-8", errors="replace").strip()

        if result.returncode != 0:
            logger.error(f"{name} failed with exit code {result.returncode}")
            logger.error("Error output:\n%s", error_output)
            return {"success": False, "error": error_output, "output": output}
        else:
            logger.info(f"{name} completed successfully.")
            if output:
                logger.info("Output:\n%s", output)
            return {"success": True, "output": output}
    except FileNotFoundError:
        msg = f"Executable not found at {exe_path}"
        logger.error(msg)
        return {"success": False, "error": msg}
    except Exception as e:
        msg = f"Error launching {name}: {e}"
        logger.exception(msg)
        return {"success": False, "error": msg}

@task
def transcripts_task():
    return runExe([EXTRACTOR_PATH, "transcripts"])

@task
def gpa_task():
    return runExe([EXTRACTOR_PATH, "gpa"])

@task
def enrollment_task():
    return runExe([EXTRACTOR_PATH, "enrollment"])

@task 
def comments_task():
    return runExe([EXTRACTOR_PATH, "comments"])

@task
def parents_task():
    return runExe([EXTRACTOR_PATH, "parents"])

@task
def mailsync_task():
    return runExe([MAILSYNC_PATH])


@flow(task_runner=SequentialTaskRunner())
def run_mailsync_go():
    logger = get_run_logger()
    parents_result = parents_task()
    if not parents_result.get("success"):
        logger.error("Stopping flow due to parent task failure.")
        return
    mailsync_result = mailsync_task()
    if not mailsync_result.get("success"):
        logger.error("Mail sync task failed.")

@flow
def run_attendance_go():
    runExe([EXTRACTOR_PATH, "attendance"])

@flow(task_runner=SequentialTaskRunner())
def run_transcripts_go():
    logger = get_run_logger()
    transcripts_result = transcripts_task()
    if not transcripts_result.get("success"):
        logger.error("Stopping flow due to parent task failure.")
        return
    gpa_result = gpa_task()
    if not gpa_result.get("success"):
        logger.error("Stopping flow due to parent task failure.")
        return
    comments_result = comments_task()
    if not comments_result.get("success"):
        logger.error("Stopping flow due to parent task failure.")
        return

if __name__ == "__main__":
    serve(
        run_attendance_go.to_deployment(name="run_attendance_go", interval=86400),
        run_transcripts_go.to_deployment(name="run_transcripts_go", interval=86400),
        run_mailsync_go.to_deployment(name="run_mailsync_go", interval=86400),
    )
