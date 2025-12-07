# ============================================
# 설정
# ============================================
PROJECT_DIR="/home/orange/332project"

DATASET="small"
DATA_INPUT="/dataset/${DATASET}"
DATA_OUTPUT="/home/orange/out"
MASTER_IP="2.2.2.254"
RECORDS_PER_WORKER=100000

DEFAULT_NUM_WORKERS=5

# ALL_WORKERS=("vm01" "vm02" "vm03" "vm04" "vm05" "vm06" "vm07" "vm08" "vm09" "vm10" "vm11" "vm12" "vm13" "vm14" "vm15" "vm16" "vm17" "vm18" "vm19" "vm20")
ALL_WORKERS=("vm14" "vm16" "vm17" "vm18" "vm19")

RECORD_SIZE=100

# ============================================
# 함수 정의
# ============================================

reclone_workers() {
  echo "=== Re-cloning project on all workers (rm -rf + git clone) ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host"
    ssh $host "
      rm -rf $PROJECT_DIR && \
      git clone https://github.com/dommanga/332project.git $PROJECT_DIR && \
      mkdir -p $DATA_INPUT $DATA_OUTPUT
    "
  done
  echo "✅ Re-clone complete"
}

get_workers() {
  local num=$1
  WORKERS=("${ALL_WORKERS[@]:0:$num}")
  echo "Using $num workers: ${WORKERS[*]}"
}

# 1. 초기 설정 (최초 1회)
init_workers() {
  echo "=== Initializing workers (git clone, directories) ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host"
    ssh $host "git clone https://github.com/dommanga/332project.git $PROJECT_DIR 2>/dev/null || echo 'Already cloned'"
    ssh $host "mkdir -p $DATA_INPUT $DATA_OUTPUT"
  done
  echo "✅ Init complete"
}

# 2. 코드 업데이트 (개발 중 자주 사용)
update_code() {
  echo "=== Updating code on all workers ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host: git pull && sbt compile"
    ssh $host "cd $PROJECT_DIR && git pull origin main && sbt compile"
  done
  echo "✅ Code update complete"
}

# 3. gensort + valsort 배포 (최초 1회)
deploy_gensort() {
  echo "=== Deploying gensort and valsort to workers ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host"
    scp $PROJECT_DIR/gensort $host:$PROJECT_DIR/
    scp $PROJECT_DIR/valsort $host:$PROJECT_DIR/
  done
  echo "✅ gensort and valsort deployed"
}

# 4. 테스트 데이터 생성
generate_data() {
  echo "=== Generating test data ==="
  for i in "${!WORKERS[@]}"; do
    host="${WORKERS[$i]}"
    start=$((i * RECORDS_PER_WORKER))
    echo "→ $host: generating $RECORDS_PER_WORKER records (start=$start)"
    ssh $host "rm -f $DATA_INPUT/* && cd $PROJECT_DIR && ./gensort -a -b$start $RECORDS_PER_WORKER $DATA_INPUT/data"
  done
  echo "✅ Data generation complete"
}

# 5. 출력 디렉토리 초기화
clean_output() {
  echo "=== Cleaning output directories ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host"
    ssh $host "rm -rf $DATA_OUTPUT/*"
  done
  echo "✅ Output cleaned"
}

# 6. Worker 실행
start_workers() {
  if [ -z "$MASTER_PORT" ]; then
    echo "❌ Require MASTER_PORT"
    echo "   Ex: $0 start 3 51324"
    exit 1
  fi

  echo "=== Starting workers (master: $MASTER_IP:$MASTER_PORT) ==="

  if [ -n "$FAULT_INJECT_PHASE" ] || [ -n "$FAULT_INJECT_WORKER" ]; then
    echo "⚙️  Fault injection env:"
    echo "    FAULT_INJECT_PHASE=$FAULT_INJECT_PHASE"
    echo "    FAULT_INJECT_WORKER=$FAULT_INJECT_WORKER"
  else
    echo "⚙️  Fault injection disabled (no FAULT_INJECT_* env set)"
  fi

  for host in "${WORKERS[@]}"; do
    echo "→ Starting worker on $host"

    if [ -n "$FAULT_INJECT_PHASE" ] || [ -n "$FAULT_INJECT_WORKER" ]; then
      remote_env_cmd="env"
      if [ -n "$FAULT_INJECT_PHASE" ]; then
        remote_env_cmd="$remote_env_cmd FAULT_INJECT_PHASE='$FAULT_INJECT_PHASE'"
      fi
      if [ -n "$FAULT_INJECT_WORKER" ]; then
        remote_env_cmd="$remote_env_cmd FAULT_INJECT_WORKER='$FAULT_INJECT_WORKER'"
      fi
    else
      remote_env_cmd="env -u FAULT_INJECT_PHASE -u FAULT_INJECT_WORKER"
    fi

    ssh $SSH_OPTS "$host" "
      cd $PROJECT_DIR && \
      $remote_env_cmd \
      nohup sbt \
        -J-Xms2G \
        -J-Xmx4G \
        -J-XX:MaxDirectMemorySize=8G \
        -J-XX:+UseG1GC \
        -J-XX:MaxGCPauseMillis=200 \
        \"runMain worker.WorkerClient $MASTER_IP:$MASTER_PORT -I $DATA_INPUT -O $DATA_OUTPUT\" \
        > /tmp/worker.log 2>&1 &
    " &
  done

  echo "✅ Workers started"
}

restart_worker() {
  local host="$1"

  if [ -z "$host" ] || [ -z "$MASTER_PORT" ]; then
    echo "Usage: $0 restart <host> <master_port>"
    echo "  예: $0 restart vm17 38278"
    exit 1
  fi

  echo "=== Restarting worker on $host (master: $MASTER_IP:$MASTER_PORT) ==="

  ssh $SSH_OPTS "$host" "
    cd $PROJECT_DIR && \
    env -u FAULT_INJECT_PHASE -u FAULT_INJECT_WORKER \
    nohup sbt \
      -J-Xms2G \
      -J-Xmx4G \
      -J-XX:MaxDirectMemorySize=8G \
      -J-XX:+UseG1GC \
      -J-XX:MaxGCPauseMillis=200 \
      \"runMain worker.WorkerClient $MASTER_IP:$MASTER_PORT -I $DATA_INPUT -O $DATA_OUTPUT\" \
      > /tmp/worker.log 2>&1 &
  "

  echo "✅ Restart command sent to $host"
}

stop_workers() {
  echo "=== Stopping workers ==="
  for host in "${WORKERS[@]}"; do
    echo "→ $host"
    ssh $SSH_OPTS "$host" 'pkill -f "worker.WorkerClient" || echo "  (no worker process)"'
  done
  echo "✅ Workers stopped"
}

show_logs() {
  echo "=== Tail worker logs (last 50 lines) ==="
  for host in "${WORKERS[@]}"; do
    echo "----------------------------------------"
    echo "[$host] /tmp/worker.log"
    ssh $SSH_OPTS "$host" "
      if [ -f /tmp/worker.log ]; then
        tail -n 50 /tmp/worker.log
      else
        echo '  (no /tmp/worker.log)'
      fi
    "
  done
  echo "✅ Logs shown"
}

check_results() {
  echo "=== Checking output & valsort on workers ==="
  local total_input_bytes=0
  local total_output_bytes=0

  for host in "${WORKERS[@]}"; do
    echo "----------------------------------------"
    echo "→ $host"

    ssh $SSH_OPTS "$host" "
      cd '$PROJECT_DIR'

      if [ ! -x ./valsort ]; then
        echo '  (valsort not found or not executable at ./valsort)'
        exit 0
      fi

      echo '  ▶ Running valsort per partition...'
      ok=1
      have_files=0

      for f in $DATA_OUTPUT/partition.*; do
        if [ ! -e \"\$f\" ]; then
          continue
        fi

        have_files=1
        base=\$(basename \"\$f\")
        echo \"    - checking \$base\"

        ./valsort \"\$f\" >/tmp/valsort.log 2>&1
        rc=\$?

        if [ \"\$rc\" -ne 0 ]; then
          echo \"      ❌ valsort FAILED for \$base\"
          ok=0
        else
          echo \"      ✅ valsort OK for \$base\"
        fi
      done

      if [ \"\$have_files\" -eq 0 ]; then
        echo '  (no partition.* files to check)'
        exit 0
      fi

      if [ \"\$ok\" -eq 1 ]; then
        echo '  ✅ valsort OK for all partitions on this worker'
      else
        echo '  ❌ valsort FAILED for one or more partitions (see /tmp/valsort.log)'
      fi
    "

    input_bytes=$(ssh $SSH_OPTS "$host" "
      if [ -d '$DATA_INPUT' ] && ls '$DATA_INPUT'/* >/dev/null 2>&1; then
        cat '$DATA_INPUT'/* 2>/dev/null | wc -c
      else
        echo 0
      fi
    ")

    output_bytes=$(ssh $SSH_OPTS "$host" "
      if [ -d '$DATA_OUTPUT' ] && ls '$DATA_OUTPUT'/* >/dev/null 2>&1; then
        cat '$DATA_OUTPUT'/* 2>/dev/null | wc -c
      else
        echo 0
      fi
    ")

    total_input_bytes=$(( total_input_bytes + input_bytes ))
    total_output_bytes=$(( total_output_bytes + output_bytes ))
  done

  echo "========================================"
  echo "=== Global record counts across workers ==="

  if [ -z "$RECORD_SIZE" ]; then
    RECORD_SIZE=100
  fi

  local total_input_records=$(( total_input_bytes / RECORD_SIZE ))
  local total_output_records=$(( total_output_bytes / RECORD_SIZE ))

  echo "  Input : $total_input_records records  (bytes: $total_input_bytes)"
  echo "  Output: $total_output_records records (bytes: $total_output_bytes)"

  if [ "$total_input_records" -eq "$total_output_records" ]; then
    echo "  ✅ Input and output record counts MATCH"
  else
    echo "  ❌ Input and output record counts DO NOT MATCH"
  fi

  echo "✅ Result check complete"
}

# 7. 전체 초기화 (데이터 + 출력)
reset_all() {
  clean_output
}

# ============================================
# 사용법 출력
# ============================================
usage() {
  echo "Usage: $0 <command> [num_workers] [master_port]"
  echo ""
  echo "Commands:"
  echo "  reclone     - Remove project dir and fresh git clone on workers"
  echo "  init        - Initial setup (git clone, mkdir input/output dirs)"
  echo "  update      - git pull origin main && sbt compile on workers"
  echo "  gensort     - Copy gensort and valsort binaries to workers"
  echo "  gendata     - Generate test input data on workers (uses gensort)"
  echo "  clean       - Clean output directories (DATA_OUTPUT) on workers"
  echo "  reset       - Clean output (and can be extended to regen data)"
  echo "  start       - Start all workers (requires num_workers, master_port)"
  echo "  restart     - Restart a single worker: restart <host> <master_port>"
  echo "  stop        - Kill worker processes on all selected workers"
  echo "  logs        - Tail /tmp/worker.log on all workers"
  echo "  check       - Run valsort + global input/output record count check"
  echo "  all         - update + reset (prepare workers before a run)"
  echo ""
  echo "Examples:"
  echo "  $0 init"
  echo "  $0 update 5"
  echo "  $0 gendata 5"
  echo "  $0 clean 5"
  echo "  $0 start 5 51324              # master at $MASTER_IP:51324"
  echo "  FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=2 \\"
  echo "      $0 start 5 51324          # start with fault injection"
  echo "  $0 logs 5"
  echo "  $0 check 5"
  echo "  $0 stop 5"
  echo "  $0 restart vm17 51324"
}

# ============================================
# Worker 개수 조정
# ============================================
NUM_WORKERS=${2:-$DEFAULT_NUM_WORKERS}
MASTER_PORT=${3:-""}
get_workers $NUM_WORKERS

# ============================================
# 명령어 실행
# ============================================
case "$1" in
  reclone)
    reclone_workers
    ;;
  init)
    init_workers
    ;;
  update)
    update_code
    ;;
  gensort)
    deploy_gensort
    ;;
  gendata)
    generate_data
    ;;
  clean)
    clean_output
    ;;
  reset)
    reset_all
    ;;
  start)
    start_workers
    ;;
  restart)
    # $2 = host, $3 = master_port
    MASTER_PORT="$3"
    restart_worker "$2"
    ;;
  stop)
    stop_workers
    ;;
  logs)
    show_logs
    ;;
  check)
    check_results
    ;;
  all)
    update_code
    reset_all
    echo ""
    echo "Ready to start! Run Master first, then:"
    echo "  $0 start"
    ;;
  *)
    usage
    exit 1
    ;;
esac