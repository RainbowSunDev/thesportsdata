import asyncio
from aiscore import get_data

async def periodic_task():
    while True:
        print("timer start for 2s")
        # Fetch a keyword from the database, perform a search, 
        await get_data()
        # # Sleep for 2 seconds before the next iteration
        await asyncio.sleep(2)

async def main():
    await periodic_task()
# Run the program
if __name__ == "__main__":
    asyncio.run(main())