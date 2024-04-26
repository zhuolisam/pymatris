import asyncio


async def throw_excep():
    raise Exception("This is an exception")


async def no_excep():
    try:
        await throw_excep()
    except Exception as e:
        raise e


async def main():
    await no_excep()


asyncio.get_event_loop().run_until_complete(main())
