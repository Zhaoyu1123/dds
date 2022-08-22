package utils

// fibonacci returns successive Fibonacci numbers starting from 1
func fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

// FibonacciNext returns next number in Fibonacci sequence greater than start
func FibonacciNext(start int) int {
	fib := fibonacci()
	num := fib()
	for num <= start {
		num = fib()
	}
	return num
}

func BackOff(start, max int) int {
	num := FibonacciNext(start)
	if num > max {
		return 1
	}
	return num
}
