# 332project (Week 3 milestone)

## What works (Week 3)
- Project scaffold (master/worker/common/rpc)
- Build runs cleanly with sbt (global isolation)
- Mock Master/Worker runMain entries
- Binary record utils: 100-byte record I/O, 10-byte key comparison
- Unit tests: unsigned lexicographic key compare (Scalatest)

## How to run
```bash
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" compile
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain master.MasterServer"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerClient"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" test


cat > README.md <<'EOF'
# 332project (Week 3 milestone)

## What works (Week 3)
- Project scaffold (master/worker/common/rpc)
- Build runs cleanly with sbt (global isolation)
- Mock Master/Worker runMain entries
- Binary record utils: 100-byte record I/O, 10-byte key comparison
- Unit tests: unsigned lexicographic key compare (Scalatest)

## How to run
```bash
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" compile
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain master.MasterServer"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerClient"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" test

# 332project (Week 3 milestone)

## What works (Week 3)
- Project scaffold (master/worker/common/rpc)
- Build runs cleanly with sbt (global isolation)
- Mock Master/Worker runMain entries
- Binary record utils: 100-byte record I/O, 10-byte key comparison
- Unit tests: unsigned lexicographic key compare (Scalatest)

## How to run
```bash
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" compile
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain master.MasterServer"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerClient"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" test

