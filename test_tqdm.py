from tqdm import tqdm
import random
import time


## Test 1
def random_error(n):
    randgues = random.randint(0, 10)
    if randgues < n:
        tqdm.write("Error")
    else:
        tqdm.write("No problem")
    time.sleep(0.5)


def configure_tqdm(total):
    return tqdm(
        initial=0,
        dynamic_ncols=True,
        total=total,
    )


def test_tqdm():
    range_ = range(10)
    main_pb = configure_tqdm(len(range_))
    for i in range_:
        random_error(i)
        main_pb.update(1)


## Test 2


def subtask(i, sub_pb):
    sub_pb = tqdm(total=5, position=i, leave=False)

    for i in range(1, 6):
        randgues = random.randint(0, 10)
        if randgues < i:
            sub_pb.write(f"Smaller than {i}")
        else:
            sub_pb.write("No problem")
        time.sleep(0.5)
        sub_pb.update(1)


def configure_tqdm2(total):
    return tqdm(dynamic_ncols=True, total=total, position=0, leave=True)


def test_tqdm2():
    range_ = range(1, 3)
    main_pb = configure_tqdm2(len(range_))
    for i in range_:
        subtask(i, main_pb)
        main_pb.update(1)


if __name__ == "__main__":
    test_tqdm2()
