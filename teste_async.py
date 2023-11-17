import asyncio
import time


async def lista(number):
    print('teste{number}')
    await asyncio.sleep(1)


async def main():
    task_1 = asyncio.create_task(lista(1))
    await task_1
       

asyncio.run(main())
