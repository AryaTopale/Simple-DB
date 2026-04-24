import random

with open("../data/BIGEMP.csv", "w") as f:
    f.write("Ssn,Salary\n")
    for i in range(1, 10000):  # 200 rows
        f.write(f"{i},{random.randint(10000, 90000)}\n")

with open("../data/BIGSAL.csv", "w") as f:
    f.write("Eid,Bonus\n")
    for i in range(1, 10000):  # 200 rows
        f.write(f"{i},{random.randint(1000, 20000)}\n")
# ```

# Then run:
# ```
# LOAD BIGEMP
# LOAD BIGSAL
# SETBUFFER 3
# Result <- JOIN BIGEMP, BIGSAL ON BIGEMP.Ssn == BIGSAL.Eid
# PRINT Result