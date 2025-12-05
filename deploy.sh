# ============================================
# 설정
# ============================================
PROJECT_DIR="/home/orange/332project"

DATASET="small"
DATA_INPUT="/dataset/${DATASET}"
DATA_OUTPUT="/home/orange/out"
MASTER_IP="2.2.2.254"
MASTER_PORT="5100"
RECORDS_PER_WORKER=100000

DEFAULT_NUM_WORKERS=2

# ALL_WORKERS=("vm01" "vm02" "vm03" "vm04" "vm05" "vm06" "vm07" "vm08" "vm09" "vm10" "vm11" "vm12" "vm13" "vm14" "vm15" "vm16" "vm17" "vm18" "vm19" "vm20")
ALL_WORKERS=("vm17" "vm18")

# ============================================
# 함수 정의
# ============================================

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
  echo "=== Starting workers ==="
  for host in "${WORKERS[@]}"; do
    echo "→ Starting worker on $host"
    ssh $host "cd $PROJECT_DIR && sbt 'runMain worker.WorkerClient $MASTER_IP:$MASTER_PORT -I $DATA_INPUT -O $DATA_OUTPUT'" &
  done
  echo "✅ Workers started (running in background)"
}

# 7. 전체 초기화 (데이터 + 출력)
reset_all() {
  clean_output
}

# ============================================
# 사용법 출력
# ============================================
usage() {
  echo "Usage: $0 <command> [num_workers]"
  echo ""
  echo "Commands:"
  echo "  init        - Initial setup (git clone, mkdir)"
  echo "  update      - Git pull && sbt compile on all workers"
  echo "  gensort     - Deploy gensort and valsort binaries to workers"
  echo "  gendata     - Generate test data on workers"
  echo "  clean       - Clean output directories"
  echo "  reset       - Clean output + generate new data"
  echo "  start       - Start all workers"
  echo "  all         - Full deployment (update + reset + start)"
  echo ""
  echo "Options:"
  echo "  num_workers - Number of workers to use (default: ${#WORKERS[@]})"
  echo ""
  echo "Examples:"
  echo "  $0 init"
  echo "  $0 update"
  echo "  $0 gendata"
  echo "  $0 start"
  echo "  $0 all 5     # Use 5 workers"
}

# ============================================
# Worker 개수 조정
# ============================================
NUM_WORKERS=${2:-$DEFAULT_NUM_WORKERS}
get_workers $NUM_WORKERS

# ============================================
# 명령어 실행
# ============================================
case "$1" in
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