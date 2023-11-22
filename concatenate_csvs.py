import os
import csv

if __name__ == "__main__":
    with open("result.csv", "w", encoding="UTF-8") as file:
        for filename in os.listdir():
            if ".csv" not in filename:
                continue
            with open(filename, "r", encoding="UTF-8") as read_file:
                data = read_file.readlines()[1:]
            for _d in data:
                file.write(_d)
