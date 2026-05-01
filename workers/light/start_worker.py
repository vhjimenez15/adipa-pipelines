import subprocess
import sys

if __name__ == "__main__":
    subprocess.run(
        ["prefect", "worker", "start", "--pool", "light-pool"],
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
