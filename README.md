# bbolt_bench
Benchmark tool for bbolt


## Steps to run benchmark against a specific bbolt version
### Step 1: Update go.mod/go.sum to depend on the version

Example 1: get a specific commit on main branch
```
$ go get go.etcd.io/bbolt@67165811e57a79678b6fab9b029bc032b9dfef0e
```

Example 2: get 1.3.8
```
$ go get go.etcd.io/bbolt@v1.3.8
```

### Step 2: build

```
$ go build
```

### Step 3: Run benchmark

Example:
```
$ ./bbolt_bench -count 100000 -batch-size 25000
```
