
## Coding Questions
<h4> Q1. Write an SQL query to find the second-highest salary from an "Employees" table. </h4>

SELECT salary
FROM Employees
ORDER BY salary DESC
LIMIT 1
OFFSET 1;

<h4> Q2. Write a MapReduce program to calculate the word count of a given input text file. </h4>

```
import sys

def mapper(key, value):
    for word in value.split():
        yield word, 1

def reducer(key, values):
    total = sum(values)
    yield key, total

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, "r") as input_file:
        with open(output_file, "w") as output_file:
            writer = output_file.write

            map_reduce = map(mapper, input_file, sys.stdin)
            for key, value in reduce(map_reduce):
                writer(f"{key}:{value}\n")
```

#### Q3. Write a Spark program to count the number of occurrences of each word in a given text file.
```
import sys

def word_count(text):
    words = text.split()
    return {word: words.count(word) for word in words}

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, "r") as input_file:
        data = input_file.read()

    word_counts = sc.parallelize(data).mapPartitions(word_count).collect()

    with open(output_file, "w") as output_file:
        for word, count in word_counts:
            output_file.write(f"{word}:{count}\n")
```
