import asyncio


async def task(i):
    await asyncio.sleep(1)
    print(f"Hello, World! {i}")


def main():
    loop = asyncio.get_event_loop()
    t = loop.create_task(task(1))
    loop.run_until_complete(task(2))
    loop.run_until_complete(t)
    asyncio.get_event_loop()


if __name__ == "__main__":
    main()
