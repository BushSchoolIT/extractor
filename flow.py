import os
import sys
import io
import subprocess
import pathlib
from multiprocessing import Process
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import Deployment

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE_DIR = pathlib.Path(__file__).parent.resolve()
EXTRACTOR_PATH = os.getenv("EXTRACTOR_PATH", str(BASE_DIR / "bbextract"))
MAILSYNC_PATH = os.getenv("MAILSYNC_PATH", str(BASE_DIR / "mailsync"))
WORK_POOL = "work_pool_0"

# Setup prefect API URL
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"


def start_worker():
    subprocess.run(["prefect", "worker", "start","--pool", WORK_POOL])

def runExe(args: list[str]):
    exe_path = args[0]
    exe_dir = os.path.dirname(exe_path)
    name = os.path.basename(exe_path)
    logger = get_run_logger()

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
        raise Exception(f"{name} failed with code {result.returncode}")
    else:
        logger.info(f"{name} completed successfully.")
        if output:
            logger.info("Output:\n%s", output)

@task
def transcripts_task():
    runExe([EXTRACTOR_PATH, "transcripts"])

@task
def gpa_task():
    runExe([EXTRACTOR_PATH, "gpa"])

@task
def enrollment_task():
    runExe([EXTRACTOR_PATH, "enrollment"])

@task 
def comments_task():
    runExe([EXTRACTOR_PATH, "comments"])

@task
def parents_task():
    runExe([EXTRACTOR_PATH, "parents"])

@task
def mailsync_task():
    runExe([MAILSYNC_PATH])


@flow(task_runner=SequentialTaskRunner())
def run_mailsync_go():
    parents_task()
    mailsync_task()

@flow
def run_attendance_go():
    runExe([EXTRACTOR_PATH, "attendance"])

@flow(task_runner=SequentialTaskRunner())
def run_transcripts_go():
    transcripts_task()
    comments_task()
    gpa_task()

if __name__ == "__main__":
    Deployment.build_from_flow(
        flow=run_attendance_go,
        name="run_attendance_go",
        work_pool_name=WORK_POOL,
        schedule={"interval": 86400}
    ).apply()

    Deployment.build_from_flow(
        flow=run_transcripts_go,
        name="run_transcripts_go",
        work_pool_name=WORK_POOL,
        schedule={"interval": 86400}
    ).apply()

    Deployment.build_from_flow(
        flow=run_mailsync_go,
        name="run_mailsync_go",
        work_pool_name=WORK_POOL,
        schedule={"interval": 86400}
    ).apply()

    p = Process(target=start_worker)
    p.start()
    p.join()
