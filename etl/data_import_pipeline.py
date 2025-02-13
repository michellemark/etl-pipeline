import os

PREFECT_API_KEY = os.environ.get("PREFECT_API_KEY")

from prefect import flow, task


@task
def print_hello(name):
    print(f"Hello {name} from a Prefect task! 🤗")


@task
def print_goodbye(name):
    print(f"Goodbye {name} from a Prefect task! 👋")


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print_hello(name)

    if goodbye:
        print_goodbye(name)


if __name__ == "__main__":
    hello_world.serve(name="my-first-deployment",
                      tags=["test-run"],
                      parameters={"goodbye": True},
                      interval=60)
