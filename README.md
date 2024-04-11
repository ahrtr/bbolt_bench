# bbolt_bench

## Steps to run benchmark against a specific bbolt version
### Step 1: Update go.mod/go.sum to depend on the target bbolt version

You need to update the go.mod/go.sum to depend on the target bbolt version, against which you are going to run benchmark.

Example 1: get a specific commit on main branch
```
$ go get go.etcd.io/bbolt@67165811e57a79678b6fab9b029bc032b9dfef0e
$ go mod tidy
```

Example 2: get 1.3.8
```
$ go get go.etcd.io/bbolt@v1.3.8
$ go mod tidy
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

### Step 4: cleanup

```
$ make clean
```

## Contribution

Any contribution or suggestion is welcome!
