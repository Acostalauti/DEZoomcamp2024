
```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)
suma = 0

for sqrt_value in generator:
    suma = sqrt_value + suma
    print(sqrt_value)

print (suma)

```
