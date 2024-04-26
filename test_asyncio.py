import asyncio
from functools import partial
from pymatris.downloader import _QueueList, Token


async def task_with_exception():
    raise ValueError("Something went wrong")


async def task_without_exception():
    await asyncio.sleep(1)
    return "Task completed successfully"


async def test_queue_token():
    queue = asyncio.Queue(maxsize=2)
    for i in range(2):
        queue.put_nowait(Token(i + 1))

    tasks = _QueueList()
    tasks.extend(
        [task_without_exception(), task_with_exception(), task_without_exception()]
    )

    tasks_queue = tasks.generate_queue()

    futures = []
    while not tasks_queue.empty():
        task = await tasks_queue.get()
        token = await queue.get()
        future = asyncio.create_task(task)

        def callback(token, future):
            try:
                queue.put_nowait(token)
            except asyncio.CancelledError:
                return

        future.add_done_callback(partial(callback, token))
        futures.append(future)

    # try:
    #     done, _ = await asyncio.wait(futures)
    # except ValueError as e:
    #     print("Caught:", e)

    result = await asyncio.gather(*futures, return_exceptions=True)
    return result


async def main():
    try:
        results = await asyncio.gather(task_with_exception(), task_without_exception())
        print("Result:", results)
    except ValueError as e:
        print("Helooooo Caught:", e)


async def main2():
    try:
        suc, pending = await asyncio.wait(
            [task_with_exception(), task_without_exception()]
        )
        print("Result:", suc)
    except ValueError as e:
        print("Helooooo:", e)


async def main3():
    try:
        result = await test_queue_token()
        print("Result:", result)
        for r in result:
            if isinstance(r, Exception):
                print("Caught:", r)
    except ValueError as e:
        print("Hey I catched you:", e)


asyncio.run(main3())
