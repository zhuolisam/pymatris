import aiofiles


async def async_write_worker(queue, file_pb, filepath):
    async with aiofiles.open(filepath, mode="wb") as f:
        while True:
            offset, chunk = await queue.get()

            await f.seek(offset)
            await f.write(chunk)
            await f.flush()

            # Update the progressbar for file
            if file_pb is not None:
                file_pb.update(len(chunk))

            queue.task_done()
